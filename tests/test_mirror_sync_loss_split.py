"""What mirror_sync's lost rows CAN mean (AR-184, ledger R1340).

On 2026-10-01 a sync reported 365,459 rows "lost ... followed the publisher". Measured afterwards,
most were not withdrawals: insee_bdm's 27,305 were values revised in place (idbank was not a key
candidate, so the whole row was the identity), defillama's 23,246 were attribute refreshes, and
many of ilostat's were the same observation under a new series_key.

These pin: idbank keys the comparison (so a revised insee value is not a loss); a keyed loss is
reported as absent identities that may be withdrawn OR re-keyed, never as withdrawals; a whole-row
loss says a revision, a rename and a removal all count; the ledger and the summary say it; a failed
breakdown never blocks the replace and is never silent. There is deliberately NO re-key count: a
row-level test found 0 of ilostat's re-keys and over-claimed 7,984 on fed_board (see loss_breakdown).
"""
import contextlib
import datetime as dt
import io
import os
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import mirror_sync as ms  # noqa: E402

D1, D2, D3 = dt.date(2024, 1, 1), dt.date(2024, 4, 1), dt.date(2024, 7, 1)


def _w(tmp_path, name, table):
    p = str(tmp_path / f"{name}.parquet")
    pq.write_table(table, p)
    return p


def _insee(rows):
    """insee_bdm's real schema: exactly dataflow / idbank / obs_date / value."""
    return pa.table({"dataflow": [r[0] for r in rows], "idbank": [r[1] for r in rows],
                     "obs_date": pa.array([r[2] for r in rows], pa.date32()),
                     "value": [r[3] for r in rows]})


def _ilo(rows):
    """ilostat's shape: a series_key that embeds the survey code, plus dimension columns."""
    return pa.table({"series_key": [r[0] for r in rows], "ref_area": [r[1] for r in rows],
                     "sex": [r[2] for r in rows],
                     "obs_date": pa.array([r[3] for r in rows], pa.date32()),
                     "value": [r[4] for r in rows]})


# ---- idbank ---------------------------------------------------------------------------------------

def test_insee_revisions_are_not_losses_once_idbank_keys_the_comparison(tmp_path):
    """27,305 revised values were reported as lost rows: without idbank the whole row was the key."""
    a = _w(tmp_path, "a", _insee([("CNA", "001", D1, 1.0), ("CNA", "001", D2, 2.0), ("CNA", "002", D1, 5.0)]))
    b = _w(tmp_path, "b", _insee([("CNA", "001", D1, 1.1), ("CNA", "001", D2, 2.2), ("CNA", "002", D1, 5.5)]))
    n, mode = ms.lost_identities(a, b)
    assert n == 0 and mode.startswith("(idbank, obs_date)"), (n, mode)


def test_control_an_insee_observation_really_gone_still_counts(tmp_path):
    a = _w(tmp_path, "a", _insee([("CNA", "001", D1, 1.0), ("CNA", "001", D2, 2.0)]))
    b = _w(tmp_path, "b", _insee([("CNA", "001", D1, 1.0)]))
    assert ms.lost_identities(a, b)[0] == 1


def test_the_key_list_includes_idbank_and_the_bare_key():
    assert set(ms.KEY_CANDIDATES) == {"series_key", "series_id", "idbank", "key"}


# ---- the breakdown --------------------------------------------------------------------------------

def test_a_keyed_loss_is_absent_identities_with_the_row_totals(tmp_path):
    local = _ilo([("EMP|GBR|BA:2247", "GBR", "T", D1, 10.0), ("EMP|GBR|BA:2247", "GBR", "T", D2, 11.0),
                  ("EMP|KEN|BX:3465", "KEN", "T", D1, 7.0)])
    incoming = _ilo([("EMP|GBR|BA:666", "GBR", "T", D1, 10.0), ("EMP|GBR|BA:666", "GBR", "T", D2, 11.0)])
    a, b = _w(tmp_path, "a", local), _w(tmp_path, "b", incoming)
    lost = ms.lost_identities(a, b)[0]
    bd = ms.loss_breakdown(a, b, lost)
    assert lost == 3 and bd == {"basis": "keyed", "local_rows": 3, "incoming_rows": 2}, (lost, bd)
    text = ms.describe_loss(lost, bd)
    assert "3 identities absent from the incoming copy" in text and "withdrawn OR re-keyed" in text, text
    assert "cannot tell apart" in text and "rows 3 -> 2" in text, text


def test_whole_row_files_say_they_cannot_be_split(tmp_path):
    local = pa.table({"chain": ["eth", "sol", "arb"], "tvl": [1.0, 2.0, 3.0]})
    incoming = pa.table({"chain": ["eth", "sol", "arb", "op"], "tvl": [1.5, 2.0, 3.5, 0.1]})
    a, b = _w(tmp_path, "a", local), _w(tmp_path, "b", incoming)
    lost = ms.lost_identities(a, b)[0]
    bd = ms.loss_breakdown(a, b, lost)
    assert lost == 2 and bd == {"basis": "whole-row", "local_rows": 3, "incoming_rows": 4}
    text = ms.describe_loss(lost, bd)
    assert "revised value" in text and "cannot be told apart" in text and "3 -> 4" in text


def test_the_basis_follows_the_identity_the_comparison_used(tmp_path):
    """insee_bdm is KEYED now (idbank); a file with no candidate column is whole-row."""
    a = _w(tmp_path, "a", _insee([("CNA", "001", D1, 1.0)]))
    assert ms.loss_breakdown(a, a, 0)["basis"] == "keyed"
    c = _w(tmp_path, "c", pa.table({"chain": ["eth"], "tvl": [1.0]}))
    assert ms.loss_breakdown(c, c, 0)["basis"] == "whole-row"


def test_describe_loss_when_the_breakdown_failed():
    assert "NOT computed" in ms.describe_loss(5, None)


# ---- what sync_source writes and prints ---------------------------------------------------------

SRC = "_testprobe_split"
ROOTDIR = "_testdata"


class StubS3:
    def __init__(self, staged):
        self.staged = staged

    def download_file(self, bucket, key, dest):
        shutil.copyfile(self.staged, dest)


def _sync(tmp_path, local, incoming, entry):
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    pq.write_table(local, os.path.join(dest_dir, "f.parquet"))
    staged = str(tmp_path / "incoming.parquet")
    pq.write_table(incoming, staged)
    rec = {"source": SRC, "root": ROOTDIR, "behind": [entry], "r2_only": [], "ahead": []}
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            pulled = ms.sync_source(StubS3(staged), rec, apply=True)[0]
        after = pq.read_metadata(os.path.join(dest_dir, "f.parquet")).num_rows
    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)
    logs = os.path.join(ms.ROOT, "logs")
    bodies = []
    for f in os.listdir(logs):
        if f.startswith("_mirror_sync_withdrawals_") and f.endswith(f"_{SRC}.tsv"):
            bodies.append(open(os.path.join(logs, f), encoding="utf-8").read())
            os.remove(os.path.join(logs, f))
    return buf.getvalue(), after, pulled, "\n".join(bodies)


def _ilo_behind():
    """local 3 rows to 2024-04; incoming 4 rows to 2024-07 - classify() says 'behind'."""
    local = _ilo([("EMP|GBR|BA:2247", "GBR", "T", D1, 10.0), ("EMP|GBR|BA:2247", "GBR", "T", D2, 11.0),
                  ("EMP|KEN|BX:3465", "KEN", "T", D1, 7.0)])
    incoming = _ilo([("EMP|GBR|BA:666", "GBR", "T", D1, 10.0), ("EMP|GBR|BA:666", "GBR", "T", D2, 11.0),
                     ("EMP|GBR|BA:666", "GBR", "T", D3, 12.0), ("EMP|FRA|BA:1", "FRA", "T", D3, 3.0)])
    return local, incoming


def test_the_summary_and_the_ledger_say_what_a_keyed_loss_can_mean(tmp_path):
    local, incoming = _ilo_behind()
    out, after, pulled, ledger = _sync(tmp_path, local, incoming, ["f", 3, 4])
    assert after == 4 and pulled == 1, (after, pulled)
    assert "1 file(s) lost 3 ROWS" in out and "NOT necessarily withdrawals" in out, out
    assert "3 identities absent from the incoming copy in 1 keyed file(s) - withdrawn OR re-keyed" in out, out
    assert "followed the publisher" not in out and "a replaced value is not counted" not in out, out
    assert "intent; 3 identities absent from the incoming copy - withdrawn OR re-keyed" in ledger, ledger
    assert "replaced with R2's copy; 3 identities absent" in ledger, ledger


def test_a_whole_row_loss_in_the_summary_says_it_cannot_be_split(tmp_path):
    local = pa.table({"chain": ["eth", "sol", "arb"], "tvl": [1.0, 2.0, 3.0]})
    incoming = pa.table({"chain": ["eth", "sol", "arb", "op"], "tvl": [1.5, 2.0, 3.5, 0.1]})
    out, after, _p, ledger = _sync(tmp_path, local, incoming, ["f", 3, 4])
    assert after == 4
    assert "2 in 1 WHOLE-ROW file(s), where a revised value, a rename and a removal all count" in out, out
    assert "rows 3 -> 4" in out and "rows 3 -> 4" in ledger, (out, ledger)


def test_a_failed_breakdown_never_blocks_the_replace_and_is_not_silent(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("footer unreadable")
    monkeypatch.setattr(ms, "loss_breakdown", boom)
    local, incoming = _ilo_behind()
    out, after, pulled, ledger = _sync(tmp_path, local, incoming, ["f", 3, 4])
    assert after == 4 and pulled == 1, "a failed description blocked the replace"
    assert "LOSS BREAKDOWN FAILED" in ledger and "footer unreadable" in ledger, ledger
    assert "3 in 1 file(s) whose breakdown FAILED - unexplained" in out, out
    assert "breakdown NOT computed" in ledger, ledger
