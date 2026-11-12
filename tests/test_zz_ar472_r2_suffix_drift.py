"""AR-472 round 2 probe: KEY-FORM DRIFT that the declared form cannot see.

SURS's table list already carries 57 ids with an UPPERCASE '.PX' suffix, and the fetcher strips only
lowercase '.px' (tid.replace('.px', '')). If a catalogued table's id comes back as 'AAA01S.PX', the
fetcher finds no stored max for 'AAA01S.PX', re-pulls the whole table under NEW keys
'SI:AAA01S.PX:...', and reports 'SI:AAA01S.PX'. The catalogue id 'stat_slovenia:SI:AAA01S' is a
PREFIX of that key, not an extension of it, so the PK-range check (ids >= key) misses it, the key
fully matches 'SI:[^:\\s]+', and the zero-mapped demote turns into a coverage note: the served CSV of
a catalogued table goes stale under a green unit (the R359 class this branch exists for)."""
import json
import os
import sys
import types

_REPO = os.environ.get("AR472_REPO") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "tests"))

import test_stat_slovenia_changed_keys as T  # noqa: E402
from updater.strategies.fetchers import stat_slovenia as S  # noqa: E402
from updater import orchestrate as O  # noqa: E402

catalog = T.catalog


def test_uppercase_px_drift_of_a_catalogued_table_must_demote(monkeypatch, tmp_path, catalog):
    st = tmp_path / "store"
    st.mkdir()
    unit = T._wire(monkeypatch, st, T._stored(), ["2024", "2025"], {"AAA01S": T._body("2025", 7.0)})
    # the publisher's id for the catalogued table AAA01S now carries '.PX'
    (st / "_catalog.json").write_text(json.dumps([{"id": "AAA01S.PX"}, {"id": "BBB01S.px"}]))
    monkeypatch.setattr(S, "_post_query", lambda sess, tid, body: T._body("2025", 7.0)
                        if tid.upper().startswith("AAA") else None)
    res = S.update(unit, None)
    print("status", res.status, "changed", res.changed_keys)
    assert set(res.changed_keys) == {"SI:AAA01S.PX"}, res.changed_keys   # control: the drift happened
    u = types.SimpleNamespace(source_id="stat_slovenia", key="stat_slovenia/_all", config={})
    out = O._derive_changed_csvs(u, res, None, None)
    print("note", out[1])
    assert (out[1] or "").startswith("csv coherence unmet"), out[1]


def test_direct_prefix_case(catalog):
    # the helper alone: a key that EXTENDS a catalogue id
    assert O._uncatalogued_by_form("stat_slovenia", ["SI:AAA01S.PX"]) is False
