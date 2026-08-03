"""ons_uk's per-run window must ROTATE, not truncate (R190).

`todo[:MAX_PER_RUN]` over a stable catalog order looks like a budget and behaves like one
only while every dataset succeeds — success advances the vintage and drops the dataset out of
`todo`. A dataset that CANNOT publish never advances, so it holds its slot forever.

That is not hypothetical here. Measured 2026-08-03 against live ONS: of the 12 datasets in the
window, 10 were permanent non-publishers (7 whose time codes no parser understood, 2 Census
tables with no time axis at all, 1 whose download was dead). 297 datasets were pending and the
queue drained at ~2 per run, with the same 10 re-downloaded every single day.

These tests pin the property that fixes it: whatever sits at the head, the NEXT run must start
somewhere else, and every dataset must eventually be reached.
"""
from updater.strategies.fetchers import ons_uk


def _rotate(todo_ids, catalog_ids, after):
    """The fetcher's rotation, isolated to the ordering decision it makes."""
    cat_pos = {ds: i for i, ds in enumerate(catalog_ids)}
    todo = list(todo_ids)
    if after in cat_pos and todo:
        k = cat_pos[after]
        n = len(cat_pos)
        todo = sorted(todo, key=lambda t: (cat_pos.get(t, 0) - k - 1) % n)
    return todo


CATALOG = [f"ds{i:03d}" for i in range(30)]


def test_without_a_cursor_the_window_is_the_head():
    assert _rotate(CATALOG, CATALOG, None)[:5] == ["ds000", "ds001", "ds002", "ds003", "ds004"]


def test_the_window_starts_after_the_saved_cursor():
    assert _rotate(CATALOG, CATALOG, "ds004")[:3] == ["ds005", "ds006", "ds007"]


def test_the_window_wraps_past_the_end_of_the_catalog():
    got = _rotate(CATALOG, CATALOG, "ds028")
    assert got[:3] == ["ds029", "ds000", "ds001"]


def test_a_cursor_no_longer_in_todo_still_positions_the_window():
    """The cursor dataset SUCCEEDS and leaves `todo` — the common case, and the one an
    integer offset into `todo` would get wrong, because `todo` shrinks underneath it."""
    todo = [d for d in CATALOG if d != "ds004"]
    assert _rotate(todo, CATALOG, "ds004")[:3] == ["ds005", "ds006", "ds007"]


def test_permanent_blockers_do_not_starve_the_rest():
    """THE regression. ds000..ds009 can never publish, so they are in `todo` every run.
    Under truncation the window is those ten forever and ds010+ is never fetched; under
    rotation every dataset is reached within one pass."""
    blockers = set(CATALOG[:10])
    todo = list(CATALOG)
    seen, after, runs = set(), None, 0
    while runs < 20 and not set(CATALOG) <= seen:
        window = _rotate(todo, CATALOG, after)[:ons_uk.MAX_PER_RUN]
        assert window, "a run must always have work while todo is non-empty"
        seen.update(window)
        after = window[-1]
        # everything that is not a blocker publishes and leaves the queue
        todo = [d for d in todo if d in blockers or d not in seen]
        runs += 1
    assert set(CATALOG) <= seen, f"only reached {len(seen)}/30 datasets in {runs} runs"
    assert runs <= 4, f"30 datasets at {ons_uk.MAX_PER_RUN}/run should take ~3 runs, took {runs}"


def test_rotation_sidecar_round_trips(tmp_path, monkeypatch):
    """The cursor is useless unless it survives to the next process."""
    import json
    store = {}

    monkeypatch.setattr(ons_uk.blob, "read_bytes", lambda p: store.get(p))
    monkeypatch.setattr(ons_uk.blob, "write_bytes_atomic",
                        lambda p, b: store.__setitem__(p, b))

    assert ons_uk._load_rotation("d") == {}          # absent -> empty, not a crash
    ons_uk._save_rotation("d", "ds017")
    assert ons_uk._load_rotation("d") == {"after": "ds017"}

    store[list(store)[0]] = b"{not json"             # corrupt -> start over, don't raise
    assert ons_uk._load_rotation("d") == {}
    json.dumps({})                                    # (keeps the import honest)
