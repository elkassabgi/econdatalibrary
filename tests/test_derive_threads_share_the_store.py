"""updater.derive.derive_and_put writes EVERY CSV to the store it is given, whatever the worker count (R1204).

With more than one worker it used to build a store per thread with blob.from_env(), whose default is
LocalBlob: with AQUEDUCT_BACKEND unset the CSVs went to relative local paths (series/<id>.csv under the
current directory) and the caller's store received nothing. A test left series/zz%3A*.csv in the checkout
that way; after T0 (store chosen by the cutover flag, variable unset) the updater's served CSVs would have
gone to local files."""
import os
import threading

import pytest

from updater import derive


class Recording:
    def __init__(self):
        self.puts, self.threads, self._lock = [], set(), threading.Lock()

    def put_atomic(self, key, data, plain=False):
        with self._lock:
            self.puts.append(key)
            self.threads.add(threading.get_ident())


@pytest.mark.parametrize("workers", ["1", "4"])
def test_every_put_reaches_the_given_store(tmp_path, monkeypatch, workers):
    monkeypatch.chdir(tmp_path)                          # a relative write would land here
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")               # the default route: from_env() would be LocalBlob
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", workers)
    monkeypatch.setenv("AQUEDUCT_DERIVE_BUDGET_MIN", "0")
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: b"series_id,obs_date,value\nk,2024-01-01,1\n")
    store = Recording()
    ids = [f"zz:{i}" for i in range(24)]
    out = derive.derive_and_put(ids, store)
    assert out["put"] == 24 and len(store.puts) == 24, (out, len(store.puts))
    assert not os.path.exists(tmp_path / "series"), "a CSV was written to a relative local path"
    if workers == "4":
        assert len(store.threads) > 1, "precondition: the puts really ran on several threads"
