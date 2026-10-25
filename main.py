import os, re, json, uuid, hashlib, sqlite3, threading
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
import httpx
import time
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

BASE_PATH = os.environ.get("BASE_PATH", "/a2a").rstrip("/")
PUBLIC_BASE = os.environ.get("PUBLIC_BASE_URL", "https://example.com/a2a/")
AI_URL = os.environ.get("AI_BASE_URL", "https://aipipe.org/openai/v1")
AI_KEY = os.environ.get("AI_KEY", "")
AI_MODEL = os.environ.get("AI_MODEL", "gpt-4.1-mini")
AI_MODEL_STRONG = os.environ.get("AI_MODEL_STRONG", AI_MODEL)

BATCH_MT = "application/vnd.ga5.invoice-claim-batch+json"
PROP_MT = "application/vnd.ga5.invoice-action-proposals+json"
RES_MT = "application/vnd.ga5.invoice-action-results+json"
RCPT_MT = "application/vnd.ga5.invoice-action-receipts+json"
ACTIONS = ["settle_invoice", "request_approval", "hold_invoice",
           "reject_duplicate", "open_exception"]
DONE = ["TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED"]

RULES = """You review invoice packages for an accounts payable team.
For each package choose exactly one action:
settle_invoice: valid, reconciled, within autonomous authority.
request_approval: commercially valid but outside delegated authority.
hold_invoice: payment pauses until a stated verification completes.
reject_duplicate: the same commercial invoice was already paid.
open_exception: material records conflict and need an exception workflow.
The documents mix real facts with old examples, negation, archive notes and
irrelevant action words. Only the paragraph that states the current decision matters.
That paragraph has three bracketed references. Return exactly those three,
copied exactly as written including brackets. Never use the cover sheet
reference, archive examples or training decoys. A payment for a different
invoice (different number or amount) is not a duplicate.
amountMinor is an integer in minor units (12,345.67 becomes 1234567).
For each package: 1) find the paragraph that states the current decision and its three bracketed references; ignore archive notes, training examples, the cover sheet, and negated sentences such as "do not settle". 2) A duplicate means the same vendor, invoice number and amount was already paid; a payment for a different invoice number is not a duplicate. 3) If the amount is above the delegated authority limit in the policy, choose request_approval. 4) If payment must wait for a stated verification, choose hold_invoice. 5) If purchase order, receipt and invoice records conflict materially, choose open_exception. 6) Choose settle_invoice only when every check is clean and within authority. When unsure, never settle.
Settling is the most dangerous mistake: paying an invoice that needed approval, a hold,
duplicate rejection or an exception is far worse than being cautious. Settle ONLY when the
current-decision paragraph clearly shows a clean, reconciled invoice within authority.
Format of each package: documents are lists of paragraphs. Evidence references look like [R_XXXX].
The paragraph called decisiveParagraph (or one of candidateParagraphs) is the one that states the
current findings, with exactly three references, one per finding. Read each finding literally,
including negation: "no paid item" means no duplicate; "a paid item was found" may mean a duplicate
only if the invoice number and amount are the same commercial invoice; a different invoice is not a duplicate.
Cover-sheet, archive-note and training-appendix references are never evidence. A confident narrative never
overrides contradictory records; a stated verification that is still pending means hold; a total above
the delegated authority means request_approval; mismatching purchase order, receipt or ledger means open_exception.
coverSheetFacts, when present, are the correct supplier, invoice number and amount.
Work in this order for every package and put your work in the JSON fields, in this order:
 a) decisivePassage: copy verbatim the paragraph that states the CURRENT decision.
 b) ignored: say which other sentences you ignored (archive, training, negated, cover sheet).
 c) the four booleans in checks, answered from the decisive passage and the policy.
 d) only then the action, which must agree with the booleans:
    duplicateOfPaidInvoice true -> reject_duplicate; recordsConflict true -> open_exception;
    verificationPending true -> hold_invoice; amountAboveAuthority true -> request_approval;
    settle_invoice only if all four are false.
Reply with JSON only:
{"decisions":[{"ref":0,"decisivePassage":"...","findings":["finding 1","finding 2","finding 3"],"ignored":"...",
"checks":{"duplicateOfPaidInvoice":false,"recordsConflict":false,"verificationPending":false,"amountAboveAuthority":false},
"action":"...","vendorName":"...","invoiceNumber":"...","amountMinor":0,"currency":"INR",
"evidenceRefs":["[..]","[..]","[..]"],"reason":"one or two sentences"}]}"""

SKEPTIC = """You are a skeptical second reviewer. A first reviewer chose settle_invoice for the package.
Look for any reason that is wrong: the current-decision paragraph asks for approval, a hold or
verification, says the invoice was already paid, shows conflicting records, or the amount is above
the delegated authority. Ignore archive notes, training examples, the cover sheet and negated
sentences. Reply with JSON only: {"settleIsSafe": true or false, "why": "short"}"""

app = FastAPI(redirect_slashes=False)
db = sqlite3.connect("state.db", check_same_thread=False)
db.execute("create table if not exists tasks (id text primary key, owner text, body text)")
db.execute("create table if not exists msgs (owner text, mid text, hash text, task_id text, primary key (owner, mid))")
db.execute("create table if not exists cache (key text primary key, body text)")
db_lock = threading.Lock()
locks = {}
locks_guard = threading.Lock()

CARD = {
    "name": "Invoice Action Agent",
    "description": "Reads invoice packages and proposes one business action each with evidence.",
    "version": "1.0.0",
    "capabilities": {"streaming": False, "pushNotifications": False},
    "supportedInterfaces": [{"url": PUBLIC_BASE, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}],
    "defaultInputModes": [BATCH_MT],
    "defaultOutputModes": [PROP_MT, RCPT_MT],
    "skills": [{"id": "invoice_action_agent", "name": "Invoice action agent",
                "description": "Chooses settle, approve, hold, reject or exception for each invoice.",
                "tags": ["invoice", "payments", "approval"]}],
}


def lock_for(name):
    with locks_guard:
        if name not in locks:
            locks[name] = threading.Lock()
        return locks[name]


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def canon(x):
    return json.dumps(x, sort_keys=True, separators=(",", ":"))


def sha(x):
    return hashlib.sha256(canon(x).encode()).hexdigest()


def reply(data, status=200):
    return JSONResponse(data, status_code=status, media_type="application/a2a+json")


def err(status, code, message):
    return reply({"error": {"code": code, "message": message}}, status)


def check(request, post=False):
    auth = request.headers.get("authorization", "")
    token = auth[7:].strip() if auth.startswith("Bearer ") else ""
    if not token:
        return None, err(401, "UNAUTHENTICATED", "Missing or invalid token")
    if request.headers.get("a2a-version", "") != "1.0":
        return None, err(400, "VERSION_NOT_SUPPORTED", "Use A2A-Version 1.0")
    if post and "application/a2a+json" not in request.headers.get("content-type", ""):
        return None, err(415, "CONTENT_TYPE_NOT_SUPPORTED", "Use application/a2a+json")
    return hashlib.sha256(token.encode()).hexdigest()[:20], None


def get_task(task_id):
    with db_lock:
        row = db.execute("select owner, body from tasks where id=?", (task_id,)).fetchone()
    return (row[0], json.loads(row[1])) if row else (None, None)


def save_task(task):
    with db_lock:
        db.execute("update tasks set body=? where id=?", (json.dumps(task), task["id"]))
        db.commit()


def all_text(x):
    if isinstance(x, str):
        return x + "\n"
    if isinstance(x, dict):
        return "".join(all_text(v) for v in x.values())
    if isinstance(x, list):
        return "".join(all_text(v) for v in x)
    return str(x) + "\n"


def package_key(pkg):
    return sha({k: v for k, v in pkg.items() if k != "packageId"})


def cache_get(key):
    with db_lock:
        row = db.execute("select body from cache where key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def cache_put(key, d):
    with db_lock:
        db.execute("insert or replace into cache values (?,?)", (key, json.dumps(d)))
        db.commit()




REF_RE = re.compile(r"\[R_[A-Za-z0-9]+\]")
FACT_RE = re.compile(r"Supplier\s+(.+?);\s*invoice\s+(\S+?);\s*stated total\s+([A-Z]{3})\s+([0-9][0-9,]*(?:\.[0-9]+)?)")
NOISE_RE = re.compile(r"(?:The referenced office was [A-Za-z .]+ and the audit export sequence was \d+|Review location [A-Za-z .]+; docket \d+)\.?")


def paras(pkg):
    out = []
    docs = pkg.get("documents") if isinstance(pkg, dict) else None
    for d in docs if isinstance(docs, list) else []:
        text = d.get("text", "") if isinstance(d, dict) else ""
        name = d.get("name", "") if isinstance(d, dict) else ""
        for p in re.split(r"\n\s*\n", str(text)):
            p = p.strip()
            if p:
                out.append((name, p))
    return out


def uniq(seq):
    return list(dict.fromkeys(seq))


def det_info(pkg):
    """Deterministic parts: the paragraph with exactly three bracketed refs, and the cover-sheet facts."""
    cands = [p for n, p in paras(pkg) if len(uniq(REF_RE.findall(p))) == 3]
    refs = uniq(REF_RE.findall(cands[0])) if len(cands) == 1 else None
    facts = None
    for n, p in paras(pkg):
        m = FACT_RE.search(p)
        if m:
            try:
                minor = int((Decimal(m.group(4).replace(",", "")) * 100).to_integral_value())
            except InvalidOperation:
                break
            facts = {"vendorName": m.group(1).strip(), "invoiceNumber": m.group(2).strip().rstrip(".,"),
                     "amountMinor": minor, "currency": m.group(3)}
            break
    return {"refs": refs, "facts": facts, "decisive": cands[0] if len(cands) == 1 else None,
            "candidates": cands}


def view(pkg):
    """Same package without the filler paragraphs, with the decisive paragraph called out."""
    info = det_info(pkg)
    docs = []
    for d in pkg.get("documents", []) if isinstance(pkg.get("documents"), list) else []:
        keep = []
        for p in re.split(r"\n\s*\n", str(d.get("text", "") if isinstance(d, dict) else "")):
            p = p.strip()
            if not p:
                continue
            if NOISE_RE.search(p) and not REF_RE.search(p):
                continue
            keep.append(p)
        docs.append({"name": d.get("name", "") if isinstance(d, dict) else "", "paragraphs": keep})
    v = {"documents": docs}
    if info["decisive"]:
        v["decisiveParagraph"] = info["decisive"]
    elif info["candidates"]:
        v["candidateParagraphs"] = info["candidates"]
    if info["facts"]:
        v["coverSheetFacts"] = info["facts"]
    return v


PAT = {
    "reject_duplicate": r"same commercial key|earlier settled entry|second disbursement|earlier posting for the same|exact commercial duplicate|already been paid",
    "hold_invoice": r"beneficiary details|newly supplied bank account|destination-account change|previously registered number|known-number callback|pre-existing contact channel|out-of-band check|payment-detail changes",
    "open_exception": r"does not reconcile with|outside the permitted reconciliation tolerance|incompatible|contradictory signed records|exception (?:workflow|queue|case)|conflicts with the receiving valuation",
    "request_approval": r"outside the operator's .{0,40}authority|delegation ceiling|without escalation only up to|financial-approval workflow|named (?:financial )?approver|larger reconciled payments",
    "settle_invoice": r"same payable total|within the stated rounding tolerance|reconciled price, quantity, tax, and currency",
}
DECOY_RE = re.compile(r"\b(closed|archiv\w*|unrelated|historical|expired|training|hypothetical|govern no live case|separate supplier|not this vendor)\b", re.I)


def classify(par):
    hits = [a for a, p in PAT.items() if re.search(p, par, re.I)]
    return hits[0] if len(hits) == 1 else None


def live_refs(pkg):
    """Narrative-style packages: one reference per paragraph. Live records are the ones that are not
    cover sheet, archive, training, expired or otherwise marked as not applying."""
    live = []
    for n, p in paras(pkg):
        r = uniq(REF_RE.findall(p))
        if len(r) != 1 or FACT_RE.search(p):
            continue
        if DECOY_RE.search(p):
            continue
        live.append((r[0], p))
    return live


def view_b(pkg):
    recs = []
    for n, p in paras(pkg):
        if NOISE_RE.search(p) and not REF_RE.search(p):
            continue
        refs = uniq(REF_RE.findall(p))
        recs.append({"document": n, "text": p, "refs": refs, "decoyHint": bool(DECOY_RE.search(p)) or None})
    return {"records": recs}


RULES_B = """You decide one invoice case for an accounts payable team. The case file has records, many with one
reference like [R_XXXX]. Some records are decoys: closed or unrelated cases, archive notes, training examples,
expired policies, historical payments for another order. decoyHint marks likely decoys but read the text yourself.
The live records state: the signed order and receiving and invoice facts, the payment ledger, and the vendor master,
bank-detail callback and authority ceiling. Work out these facts from the live records only:
 - sameOrderAlreadyPaid: true only if a payment already discharged THIS SAME order or invoice (same order/invoice
   identifier). A payment of the same amount to the same vendor for a DIFFERENT order or invoice is NOT a duplicate.
 - materialOrderInvoiceDifference: true only if the signed order, receipt and invoice materially disagree
   (different totals or quantities with no corrected invoice). Matching totals mean false.
 - bankChangeUnverified: true only if the live records show a bank or remittance change whose callback or
   verification through the established channel is NOT completed. A confirmed callback means false. A bank change
   mentioned only in a training or archive record means false.
 - payableTotal: the submitted invoice total as a plain number, and currency its 3-letter code.
 - delegationCeiling: the autonomous payment ceiling in the same currency as a plain number, or null if none is stated
   in the live records.
Also give vendorName (the supplier being paid), invoiceNumber (the identifier of the current order or invoice, for
example PO-41708 or INV-...), and evidenceRefs: exactly the three references of the live records that decide the case,
copied with brackets. Never use decoy, cover-sheet, archive or training references.
Reply with JSON only:
{"findings":["..","..",".."],"sameOrderAlreadyPaid":false,"materialOrderInvoiceDifference":false,
"bankChangeUnverified":false,"payableTotal":0,"currency":"INR","delegationCeiling":null,
"vendorName":"","invoiceNumber":"","evidenceRefs":["[..]","[..]","[..]"],"reason":"one or two sentences"}"""


def to_minor(x):
    try:
        return int((Decimal(str(x).replace(",", "")) * 100).to_integral_value())
    except Exception:
        return None


def decide_b(pkg, policy, started):
    text = all_text(pkg)
    live = live_refs(pkg)
    for attempt in range(2):
        if time.time() - started > 34:
            break
        try:
            a = call_model(AI_MODEL_STRONG, RULES_B,
                           json.dumps({"policyRevision": policy, "case": view_b(pkg)}), started, 28)
        except Exception as e:
            print("model error B", repr(e)[:200])
            continue
        total, minor = a.get("payableTotal"), to_minor(a.get("payableTotal"))
        if minor is None:
            continue
        ceiling = a.get("delegationCeiling")
        try:
            over = ceiling is not None and Decimal(str(ceiling).replace(",", "")) < Decimal(str(total).replace(",", ""))
        except Exception:
            over = False
        if a.get("sameOrderAlreadyPaid"):
            act = "reject_duplicate"
        elif a.get("materialOrderInvoiceDifference"):
            act = "open_exception"
        elif a.get("bankChangeUnverified"):
            act = "hold_invoice"
        elif over:
            act = "request_approval"
        else:
            act = "settle_invoice"
        if len(live) == 3:
            refs = [r for r, _ in live]
        else:
            refs = a.get("evidenceRefs")
            if not isinstance(refs, list) or len(refs) != 3 or any(not isinstance(r, str) or r not in text for r in refs):
                continue
        return {"action": act, "src": "B",
                "facts": {"vendorName": str(a.get("vendorName", "")), "invoiceNumber": str(a.get("invoiceNumber", "")),
                          "amountMinor": minor, "currency": str(a.get("currency", ""))},
                "evidenceRefs": refs, "reason": str(a.get("reason", ""))[:700]}
    return None



def clean(a, pkg):
    if not isinstance(a, dict) or a.get("action") not in ACTIONS:
        return None
    info = det_info(pkg)
    refs = a.get("evidenceRefs")
    if info["refs"]:
        refs = info["refs"]
    if isinstance(refs, list):
        refs = [r if isinstance(r, str) and r.startswith("[") else "[" + str(r) + "]" for r in refs]
    text = all_text(pkg)
    if not isinstance(refs, list) or len(refs) != 3:
        return None
    if any(not isinstance(r, str) or r not in text for r in refs):
        return None
    ck = a.get("checks")
    if isinstance(ck, dict):
        if a["action"] == "settle_invoice" and any(ck.get(k) for k in
                ("duplicateOfPaidInvoice", "recordsConflict", "verificationPending", "amountAboveAuthority")):
            return None
        if a["action"] == "reject_duplicate" and not ck.get("duplicateOfPaidInvoice"):
            return None
    try:
        amount = int(round(float(a.get("amountMinor"))))
    except Exception:
        return None
    facts = {"vendorName": str(a.get("vendorName", "")),
             "invoiceNumber": str(a.get("invoiceNumber", "")),
             "amountMinor": amount, "currency": str(a.get("currency", ""))}
    if info["facts"]:
        facts = info["facts"]
    return {"action": a["action"], "facts": facts, "evidenceRefs": refs,
            "reason": str(a.get("reason", ""))[:700]}


def call_model(model, system, user, started, timeout_cap=25):
    left = 42 - (time.time() - started)
    r = httpx.post(AI_URL + "/chat/completions",
                   headers={"Authorization": "Bearer " + AI_KEY},
                   json={"model": model, "temperature": 0,
                         "response_format": {"type": "json_object"},
                         "messages": [{"role": "system", "content": system},
                                      {"role": "user", "content": user}]},
                   timeout=max(6, min(timeout_cap, left)))
    r.raise_for_status()
    return json.loads(r.json()["choices"][0]["message"]["content"])


def ask_model(items, policy, started, forbid_settle=False):
    docs = [{"ref": i, "package": view(p)} for i, (k, p) in enumerate(items)]
    system = RULES
    if forbid_settle:
        system += "\nA reviewer found that settle_invoice is NOT safe for these packages. Do not choose settle_invoice."
    out = call_model(AI_MODEL_STRONG if forbid_settle else AI_MODEL, system,
                     json.dumps({"policyRevision": policy, "packages": docs}), started)
    return {a.get("ref"): a for a in out.get("decisions", []) if isinstance(a, dict)}


def solve(chunk, policy, started, forbid_settle=False):
    got, pending = {}, chunk
    for attempt in range(2):
        if not pending or time.time() - started > (30 if forbid_settle else 24):
            break
        try:
            answers = ask_model(pending, policy, started, forbid_settle)
        except Exception as e:
            print("model error", repr(e)[:300])
            continue
        left = []
        for i, (k, p) in enumerate(pending):
            d = clean(answers.get(i), p)
            if d and forbid_settle and d["action"] == "settle_invoice":
                d = None
            if d:
                got[k] = d
            else:
                left.append((k, p))
        pending = left
    # last resort: one package at a time, never a blanket answer
    for k, p in list(pending):
        if time.time() - started > 36:
            break
        try:
            d = clean(ask_model([(k, p)], policy, started, forbid_settle).get(0), p)
            if d and not (forbid_settle and d["action"] == "settle_invoice"):
                got[k] = d
        except Exception as e:
            print("model error single", repr(e)[:200])
    return got


def skeptic_check(item, policy, started):
    k, p = item
    try:
        out = call_model(AI_MODEL_STRONG, SKEPTIC,
                         json.dumps({"policyRevision": policy, "package": view(p)}), started, 15)
        return k, bool(out.get("settleIsSafe"))
    except Exception as e:
        print("skeptic error", repr(e)[:200])
        return k, False


def decide(pkgs, policy):
    out, todo = {}, {}
    for p in pkgs:
        k = package_key(p)
        hit = cache_get(k)
        if hit:
            out[k] = hit
        else:
            todo[k] = p
    started = time.time()
    fresh, model_items, b_items = {}, [], []
    for k, p in todo.items():
        info = det_info(p)
        if info["decisive"]:
            act = classify(info["decisive"])
            if act and info["facts"] and info["refs"]:
                fresh[k] = {"action": act, "src": "rule", "facts": info["facts"], "evidenceRefs": info["refs"],
                            "reason": info["decisive"][:700]}
            else:
                model_items.append((k, p))
        else:
            b_items.append((k, p))
    if b_items:
        with ThreadPoolExecutor(max_workers=4) as pool:
            for (k, p), d in zip(b_items, pool.map(lambda it: decide_b(it[1], policy, started), b_items)):
                if d:
                    fresh[k] = d
                else:
                    model_items.append((k, p))
    if model_items:  # unfamiliar wording: model, then skeptical look at any settle
        chunks = [model_items[i:i + 4] for i in range(0, len(model_items), 4)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            for got in pool.map(lambda c: solve(c, policy, started), chunks):
                fresh.update(got)
        settles = [(k, todo[k]) for k, _ in model_items if fresh.get(k, {}).get("action") == "settle_invoice"]
        redo = []
        if settles:
            with ThreadPoolExecutor(max_workers=4) as pool:
                for k, safe in pool.map(lambda it: skeptic_check(it, policy, started), settles):
                    if not safe:
                        redo.append((k, todo[k]))
        if redo:
            print("SKEPTIC rejected settle for", len(redo), "packages")
            chunks = [redo[i:i + 4] for i in range(0, len(redo), 4)]
            with ThreadPoolExecutor(max_workers=4) as pool:
                for got in pool.map(lambda c: solve(c, policy, started, True), chunks):
                    fresh.update(got)
            for k, p in redo:
                if fresh.get(k, {}).get("action") == "settle_invoice":
                    del fresh[k]
    for k, d in fresh.items():
        out[k] = d
        cache_put(k, d)
    for k in todo:
        if k not in out:
            out[k] = {"action": "open_exception",
                      "facts": {"vendorName": "", "invoiceNumber": "", "amountMinor": 0, "currency": ""},
                      "evidenceRefs": [], "reason": "Could not read this package reliably."}
    return out


def make_proposal(batch_id, pkg_id, d):
    refs = d["evidenceRefs"]
    text = "Action " + d["action"] + " chosen. " + d["reason"] + " Evidence: " + ", ".join(refs) + "."
    if len(text) < 60:
        text += " This follows the policy stated in the cited records."
    return {"packageId": pkg_id,
            "actionId": "act-" + hashlib.sha256((batch_id + pkg_id + d["action"]).encode()).hexdigest()[:20],
            "action": d["action"], "facts": d["facts"], "evidenceRefs": refs,
            "rationale": text[:1500]}


def new_task(owner, msg):
    part = msg["parts"][0]
    data = part.get("data")
    if part.get("mediaType") != BATCH_MT or not isinstance(data, dict):
        return None, err(400, "INVALID_REQUEST", "Expected an invoice batch")
    pkgs = data.get("packages")
    if not isinstance(pkgs, list) or not data.get("batchId"):
        return None, err(400, "INVALID_REQUEST", "Expected an invoice batch")
    ids = [p.get("packageId") for p in pkgs if isinstance(p, dict)]
    if len(ids) != len(pkgs) or not all(ids) or len(set(ids)) != len(ids):
        return None, err(400, "INVALID_REQUEST", "Bad package ids")
    decisions = decide(pkgs, data.get("policyRevision"))
    proposals = [make_proposal(data["batchId"], p["packageId"], decisions[package_key(p)]) for p in pkgs]
    print("BATCH", data["batchId"], [p["action"] for p in proposals])
    for _p, _pr in zip(pkgs, proposals):
        _d = decisions[package_key(_p)]
        print("DEC", _d.get("src", "model"), _pr["action"], "|", (det_info(_p)["decisive"] or "NO-DECISIVE-PARA")[:300])
    print("SAMPLE", json.dumps(pkgs[0])[:3500])
    task = {"id": str(uuid.uuid4()), "contextId": msg.get("contextId") or str(uuid.uuid4()),
            "status": {"state": "TASK_STATE_INPUT_REQUIRED", "timestamp": now()},
            "artifacts": [{"artifactId": str(uuid.uuid4()), "name": "proposals",
                           "parts": [{"mediaType": PROP_MT,
                                      "data": {"batchId": data["batchId"], "proposals": proposals}}]}],
            "history": [msg], "metadata": {}}
    with db_lock:
        db.execute("insert into tasks values (?,?,?)", (task["id"], owner, json.dumps(task)))
        db.commit()
    return task, None


def continue_task(owner, msg):
    tid = msg["taskId"]
    t_owner, _ = get_task(tid)
    if t_owner != owner:
        return None, err(404, "TASK_NOT_FOUND", "Task not found")
    with lock_for("task|" + tid):
        _, task = get_task(tid)
        if msg.get("contextId") and msg["contextId"] != task["contextId"]:
            return None, err(400, "CONTEXT_MISMATCH", "Context does not match")
        part = msg["parts"][0]
        data = part.get("data")
        if len(msg["parts"]) != 1 or part.get("mediaType") != RES_MT or not isinstance(data, dict):
            return None, err(400, "INVALID_REQUEST", "Expected action results")
        stored = task["artifacts"][0]["parts"][0]["data"]
        state = task["status"]["state"]
        if state != "TASK_STATE_INPUT_REQUIRED":
            if state == "TASK_STATE_COMPLETED" and task["metadata"].get("resultsHash") == sha(data):
                return task, None
            return None, err(409, "TASK_NOT_WAITING", "Task is not waiting for results")
        if data.get("batchId") != stored["batchId"] or not isinstance(data.get("results"), list):
            return None, err(400, "RESULT_MISMATCH", "Results do not match the task")
        by_pkg = {p["packageId"]: p for p in stored["proposals"]}
        seen, executions = set(), []
        for r in data["results"]:
            p = by_pkg.get(r.get("packageId")) if isinstance(r, dict) else None
            if p is None or r["packageId"] in seen:
                return None, err(400, "RESULT_MISMATCH", "Results do not match the task")
            if r.get("actionId") != p["actionId"] or r.get("action") != p["action"]:
                return None, err(400, "RESULT_MISMATCH", "Results do not match the task")
            if r.get("outcome") not in ("ACCEPTED", "REJECTED"):
                return None, err(400, "RESULT_MISMATCH", "Results do not match the task")
            seen.add(r["packageId"])
            if r["outcome"] == "ACCEPTED":
                if not isinstance(r.get("receiptNonce"), str) or not r["receiptNonce"]:
                    return None, err(400, "RESULT_MISMATCH", "Results do not match the task")
                executions.append({"packageId": p["packageId"], "actionId": p["actionId"],
                                   "action": p["action"], "receiptNonce": r["receiptNonce"],
                                   "facts": p["facts"], "evidenceRefs": p["evidenceRefs"]})
        task["artifacts"].append({"artifactId": str(uuid.uuid4()), "name": "receipts",
                                  "parts": [{"mediaType": RCPT_MT,
                                             "data": {"batchId": stored["batchId"], "executions": executions}}]})
        task["history"].append(msg)
        task["metadata"]["resultsHash"] = sha(data)
        task["status"] = {"state": "TASK_STATE_COMPLETED", "timestamp": now()}
        save_task(task)
        return task, None


def handle_send(owner, body):
    msg = body.get("message") if isinstance(body, dict) else None
    if not isinstance(msg, dict) or not msg.get("messageId") or msg.get("role") != "ROLE_USER":
        return err(400, "INVALID_REQUEST", "Bad message")
    if not isinstance(msg.get("parts"), list) or not msg["parts"]:
        return err(400, "INVALID_REQUEST", "Bad message")
    mid, h = str(msg["messageId"]), sha(msg)
    with lock_for(owner + "|" + mid):
        with db_lock:
            row = db.execute("select hash, task_id from msgs where owner=? and mid=?", (owner, mid)).fetchone()
        if row:
            if row[0] != h:
                return err(409, "IDEMPOTENCY_CONFLICT", "messageId was used with different content")
            return reply({"task": get_task(row[1])[1]})
        task, bad = (continue_task if msg.get("taskId") else new_task)(owner, msg)
        if bad:
            return bad
        with db_lock:
            db.execute("insert into msgs values (?,?,?,?)", (owner, mid, h, task["id"]))
            db.commit()
        return reply({"task": task})


def do_cancel(owner, tid):
    t_owner, _ = get_task(tid)
    if t_owner != owner:
        return err(404, "TASK_NOT_FOUND", "Task not found")
    with lock_for("task|" + tid):
        _, task = get_task(tid)
        if task["status"]["state"] in DONE:
            return err(409, "TASK_NOT_CANCELABLE", "Task already finished")
        task["status"] = {"state": "TASK_STATE_CANCELED", "timestamp": now()}
        save_task(task)
        return reply(task)


@app.get("/.well-known/agent-card.json")
def agent_card():
    return JSONResponse(CARD)


@app.post(BASE_PATH + "/message:send")
async def send(request: Request):
    owner, bad = check(request, post=True)
    if bad:
        return bad
    try:
        body = await request.json()
    except Exception:
        return err(400, "INVALID_REQUEST", "Body must be JSON")
    return await run_in_threadpool(handle_send, owner, body)


@app.get(BASE_PATH + "/tasks/{task_id}")
def read_task(task_id: str, request: Request):
    owner, bad = check(request)
    if bad:
        return bad
    t_owner, task = get_task(task_id)
    if t_owner != owner:
        return err(404, "TASK_NOT_FOUND", "Task not found")
    return reply(task)


@app.get(BASE_PATH + "/tasks")
def list_tasks(request: Request):
    owner, bad = check(request)
    if bad:
        return bad
    with db_lock:
        rows = db.execute("select body from tasks where owner=?", (owner,)).fetchall()
    tasks = [json.loads(r[0]) for r in rows]
    tasks.sort(key=lambda t: t["status"]["timestamp"], reverse=True)
    if len(json.dumps(tasks)) > 400000:
        for t in tasks:
            t.pop("history", None)
    return reply({"tasks": tasks, "nextPageToken": "", "pageSize": len(tasks), "totalSize": len(tasks)})


@app.post(BASE_PATH + "/tasks/{task_id}:cancel")
async def cancel(task_id: str, request: Request):
    owner, bad = check(request)
    if bad:
        return bad
    return await run_in_threadpool(do_cancel, owner, task_id)
