import json
import os
import hashlib
import sys
from datetime import datetime, timezone

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_DIR = os.environ.get("DBT_TARGET_DIR", os.path.join(ROOT, "target"))

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY")  # e.g. owner/repo in GitHub Actions

DEFAULT_RULES = {
    "tier_severity": {"gold": "sev2", "silver": "sev3", "bronze": "sev4"},
    "fail_kinds": {"test": True, "model": True, "snapshot": True},
}

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]

def gh_headers():
    if not GITHUB_TOKEN:
        raise RuntimeError("Missing GITHUB_TOKEN env var.")
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def gh_get(url, params=None):
    r = requests.get(url, headers=gh_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def gh_post(url, payload):
    r = requests.post(url, headers=gh_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def gh_patch(url, payload):
    r = requests.patch(url, headers=gh_headers(), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def load_rules():
    # Simple: allow override via JSON in env; keep MVP minimal.
    return DEFAULT_RULES

def build_node_index(manifest):
    # manifest["nodes"] contains models/tests; each has "unique_id"
    return manifest.get("nodes", {})

def get_owner_tier(node):
    meta = (node.get("meta") or {})
    owner = meta.get("owner", "unknown")
    tier = meta.get("tier", "silver")
    return owner, tier

def parse_failures(run_results, manifest):
    nodes = build_node_index(manifest)

    failures = []
    for res in run_results.get("results", []):
        status = res.get("status")
        if status not in ("fail", "error"):
            continue

        unique_id = res.get("unique_id")  # e.g. test.<project>.<name>
        node = nodes.get(unique_id, {})   # tests are nodes too
        resource_type = node.get("resource_type") or res.get("resource_type")

        # For tests, attach the model it points to (via depends_on)
        depends = (node.get("depends_on") or {}).get("nodes", [])
        attached_model = None
        for dep in depends:
            if dep.startswith("model."):
                attached_model = dep
                break

        owner, tier = get_owner_tier(nodes.get(attached_model, {})) if attached_model else ("unknown", "silver")

        failures.append({
            "unique_id": unique_id,
            "resource_type": resource_type,
            "name": node.get("name") or res.get("name") or unique_id,
            "message": res.get("message") or "",
            "attached_model": attached_model,
            "owner": owner,
            "tier": tier,
        })
    return failures

def severity_for(tier: str, rules):
    return rules["tier_severity"].get(tier.lower(), "sev3")

def issue_title(f):
    model = f["attached_model"].split(".")[-1] if f["attached_model"] else "unknown_model"
    return f"[DQ] {model}: {f['name']} failed"

def issue_fingerprint(f):
    # Stable across runs
    key = f"{f['attached_model']}|{f['unique_id']}"
    return sha1(key)

def render_issue_body(f, rules):
    now = datetime.now(timezone.utc).isoformat()
    model = f["attached_model"] or "N/A"
    sev = severity_for(f["tier"], rules)
    fp = issue_fingerprint(f)

    body = f"""### Data Quality Incident

**Severity:** `{sev}`  
**Tier:** `{f['tier']}`  
**Owner:** `{f['owner']}`  
**Fingerprint:** `{fp}`  
**Detected:** `{now}`

**Failing test/resource:** `{f['name']}`  
**Attached model:** `{model}`

#### dbt message
{(f["message"] or "").strip()[:3000]}


#### Next checks (auto-triage)
- If this is `unique`/`not_null`: check recent upstream joins or incremental filters.
- If this is `relationships`: verify dimension keys are loading before fact.
- If this is freshness: validate upstream load schedules and source freshness.

---
*Created by dq_incident_agent using dbt artifacts (`run_results.json`, `manifest.json`).*
"""
    return body

def find_existing_issue(fingerprint: str):
    # Search open issues with label dq and matching fingerprint string in body
    q = f'repo:{GITHUB_REPO} is:issue is:open label:dq "{fingerprint}"'
    url = "https://api.github.com/search/issues"
    data = gh_get(url, params={"q": q})
    items = data.get("items", [])
    return items[0] if items else None

def create_or_update_issue(f, rules):
    base_url = f"https://api.github.com/repos/{GITHUB_REPO}/issues"
    fp = issue_fingerprint(f)
    existing = find_existing_issue(fp)

    labels = ["dq", severity_for(f["tier"], rules), f"tier:{f['tier']}", f"owner:{f['owner']}"]
    title = issue_title(f)
    body = render_issue_body(f, rules)

    if existing:
        # Update body to refresh timestamp + latest message (keeps issue alive)
        issue_url = existing["url"]
        gh_patch(issue_url, {"title": title, "body": body, "labels": labels})
        print(f"Updated issue: {existing.get('html_url')}")
    else:
        created = gh_post(base_url, {"title": title, "body": body, "labels": labels})
        print(f"Created issue: {created.get('html_url')}")

def main():
    rules = load_rules()

    rr_path = os.path.join(TARGET_DIR, "run_results.json")
    mf_path = os.path.join(TARGET_DIR, "manifest.json")

    if not os.path.exists(rr_path) or not os.path.exists(mf_path):
        print(f"Missing artifacts in {TARGET_DIR}. Run dbt first.", file=sys.stderr)
        sys.exit(2)

    run_results = load_json(rr_path)
    manifest = load_json(mf_path)

    failures = parse_failures(run_results, manifest)
    if not failures:
        print("No failures found. ✅")
        return

    if not GITHUB_REPO:
        raise RuntimeError("Missing GITHUB_REPOSITORY env var (set automatically in GitHub Actions).")

    for f in failures:
        create_or_update_issue(f, rules)

    # Fail the job so CI shows red (optional)
    sys.exit(1)

if __name__ == "__main__":
    main()
