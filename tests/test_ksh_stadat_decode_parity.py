"""WU-3: the fetcher must decode KSH bytes EXACTLY as the ingester does (R333).

The fetcher read `decode("utf-8", errors="replace")` while the ingester does strict
utf-8-sig with a cp1250 fallback — so cp1250 tables minted U+FFFD keys and 1,931
catalogued series (clean accents, ingester-minted) became unservable: the resolver's
exact-equality match can never hit a mojibake-only store.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _fetcher_decode(raw: bytes) -> str:
    """The exact expression _fetch_table now uses — kept in lockstep by
    test_the_fetcher_source_carries_the_expression below."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1250", errors="replace")


def test_cp1250_bytes_decode_to_clean_hungarian():
    s = "Vas vármegye; sűrűség; idősor"          # ő/ű/á/é — the KSH accent set
    raw = s.encode("cp1250")
    assert _fetcher_decode(raw) == s
    # the OLD expression minted replacement characters from the same bytes
    assert "�" in raw.decode("utf-8", errors="replace")


def test_utf8_and_bom_pass_through():
    s = "Budapest; főváros"
    assert _fetcher_decode(s.encode("utf-8")) == s
    assert _fetcher_decode(b"\xef\xbb\xbf" + s.encode("utf-8")) == s


def test_the_fetcher_source_carries_the_expression():
    """Pin the shipped file to the ingester's pattern — if either half is edited
    away, this fails before production mints mojibake again."""
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "updater", "strategies", "fetchers", "ksh_stadat.py")
    src = open(p, encoding="utf-8").read()
    assert 'raw.decode("utf-8-sig")' in src
    assert 'raw.decode("cp1250", errors="replace")' in src
    assert 'raw.decode("utf-8", errors="replace")' not in src, (
        "the mojibake-minting decode is back")


def test_ingester_still_uses_the_same_pattern():
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "jobs", "ingest_ksh_stadat.py")
    src = open(p, encoding="utf-8").read()
    assert 'decode("utf-8-sig")' in src and 'decode("cp1250", errors="replace")' in src


def test_shipped_fetch_table_decodes_cp1250(monkeypatch):
    """The reviewer's Change 1 (R511): the substring pin alone is satisfiable by a
    COMMENT — this drives the SHIPPED _fetch_table with cp1250 bytes and asserts the
    clean accents (and no BOM, no U+FFFD) reach parse_table."""
    from updater.strategies.fetchers import ksh_stadat as F

    seen = {}
    body = "Terület;2024\nVas vármegye; sűrűség;42\n"
    monkeypatch.setattr(F.ig, "get_bytes",
                        lambda url: b"\xef\xbb\xbf" + body.encode("utf-8")
                        if "bom" in url else body.encode("cp1250"))
    def fake_parse(tid, txt):
        seen[tid] = txt
        return [("k", None, 1.0)], None
    monkeypatch.setattr(F.ig, "parse_table", fake_parse)

    tid, rows = F._fetch_table("lak0001")
    assert rows and tid == "lak0001"
    assert "vármegye" in seen["lak0001"] and "sűrűség" in seen["lak0001"]
    assert "�" not in seen["lak0001"]
