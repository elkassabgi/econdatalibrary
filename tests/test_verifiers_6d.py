"""THE STEP-6d VERIFIER LIST, ENUMERATED IN STEP 1 (docs/ECON_SELF_HOSTING_PLAN.md, section 5: "the full list is
enumerated by grep in step 1 and attached to the step-6d change").

After T0 the R2 guard stops WRITES only, and core.d1_remote still answers a plain read with the read token. So a
tool that proves "served" by reading R2 or D1 keeps running after T0 and checks the FROZEN cloud copy: every
answer it gives is about a system users no longer reach. Step 6d re-points each to the edge, or makes it refuse
after the flag. Until then this list is the inventory, and the ratchet keeps it complete: a verifier-type tool
(audit_/verify_/probe_/footer_/sample_/reconcile_/check_/measure_) that reads the cloud copy must either handle
T0 itself (a cutover check) or be listed here - a new one cannot be forgotten. Outside this repository:
hfdatalibrary's .claude/skills/adversarial-review/tools/ledger_check.py (its D1 and head_object checks)."""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERIFIER = re.compile(r"^(audit|verify|probe|footer|sample|reconcile|check|measure)_.*\.py$")
READS_THE_CLOUD = re.compile(r"r2_util|d1_remote|head_object|list_objects|_d1_json|R2Blob|workers\.dev")
HANDLES_T0 = re.compile(r"is_cut_over|refuse_if_cut_over|refuse_unless_live_checkout|CutoverRefused")

# To re-point to the edge or to refuse after the flag in step 6d (enumerated 2026-09-24):
TO_REPOINT_6D = {
    "audit_csv_staleness.py", "audit_d1_source_counts.py", "audit_d1_vs_catalog.py",
    "audit_licence_disclosure.py", "audit_r2_vs_catalog.py", "audit_rotation_progress.py",
    "audit_serving_coherence.py", "audit_site.py", "audit_untouched_files.py",
    "audit_unwritten_store_regions.py", "footer_diff.py", "sample_source_coverage.py",
    "verify_derive_parity.py", "verify_source_served.py", "verify_statcan_store_bytes.py",
}


def _scan():
    out = {}
    for f in sorted(os.listdir(os.path.join(ROOT, "tools"))):
        if not VERIFIER.match(f):
            continue
        src = open(os.path.join(ROOT, "tools", f), encoding="utf-8-sig", errors="replace").read()
        if READS_THE_CLOUD.search(src):
            out[f] = bool(HANDLES_T0.search(src))
    return out


def test_every_cloud_reading_verifier_is_listed_or_handles_t0():
    unlisted = sorted(f for f, handles in _scan().items() if not handles and f not in TO_REPOINT_6D)
    assert not unlisted, (f"these verifiers read the cloud copy and do nothing at T0 - add them to TO_REPOINT_6D "
                          f"(or make them handle T0): {unlisted}")


def test_the_list_holds_only_what_is_still_owed():
    """Shrink-only: a tool that now handles T0, stopped reading the cloud, or was removed leaves the list."""
    scan = _scan()
    stale = sorted(f for f in TO_REPOINT_6D if f not in scan or scan[f])
    assert not stale, f"no longer owed at 6d - remove from TO_REPOINT_6D: {stale}"
