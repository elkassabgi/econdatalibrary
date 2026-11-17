"""tools/served_receipt.py with a fake HTTP layer and a fake S3 - never the network.

Each verdict is shown both ways: an equal pair passes and a planted difference fails; a missing secret,
an unfetchable id and a forwarding worker each refuse or fail, never pass. The key is planted and the
captured stdout and stderr are searched for it.
"""
import gzip
import io
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import served_receipt as R  # noqa: E402

PLANTED_KEY = "edk_PLANTED_receipt_key_7f3a9c"
BASE = "https://api.example.test"
ID_A = "bis:WS_CBPOL:US"
ID_B = "fhfa:hpi_at_3zip_quarterly:010"
CSV_A = b"series_id,obs_date,value\nbis:WS_CBPOL:US,2026-09-30,4.25\nbis:WS_CBPOL:US,2026-10-31,4.0\n"
CSV_B = b"series_id,obs_date,value\nfhfa:hpi_at_3zip_quarterly:010,2026-03-31,301.2\n"


def _gz(b):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as g:
        g.write(b)
    return buf.getvalue()


class FakeHttp:
    def __init__(self, bodies, forward=False, status=None):
        self.bodies = bodies            # id -> (body bytes, headers)
        self.forward = forward
        self.status = status or {}
        self.calls = []

    def __call__(self, url, headers):
        self.calls.append((url, dict(headers)))
        if url.endswith("/v1/edge-status"):
            assert "X-API-Key" not in headers, "the key is sent only on data requests"
            return 200, {}, json.dumps({"forward": self.forward}).encode()
        assert url.endswith(".csv?raw=1")
        assert headers.get("X-API-Key") == PLANTED_KEY
        assert PLANTED_KEY not in url, "the key never goes in a URL"
        for sid, (body, hdr) in self.bodies.items():
            if url == R.served_url(BASE, sid):
                return self.status.get(sid, 200), hdr, body
        return 404, {}, b'{"error":"not_found"}'


class FakeS3:
    def __init__(self, objects):
        self.objects = objects          # key -> (raw bytes, content-encoding or None)
        self.keys = []

    def get_object(self, Bucket, Key):
        assert Bucket == R.BUCKET
        self.keys.append(Key)
        if Key not in self.objects:
            raise RuntimeError("NoSuchKey " + PLANTED_KEY)   # a message that quotes the key is redacted
        raw, enc = self.objects[Key]
        r = {"Body": io.BytesIO(raw)}
        if enc:
            r["ContentEncoding"] = enc
        return r


def _run(capsys, ids, http, s3, env=None):
    env = {R.KEY_ENV: PLANTED_KEY} if env is None else env
    rc = R.main(["--ids", " ".join(ids), "--base", BASE], get=http, s3_factory=lambda: s3, environ=env)
    out, err = capsys.readouterr()
    return rc, out, err


def _no_key(out, err):
    assert PLANTED_KEY not in out and PLANTED_KEY not in err


# ---- equal / different -------------------------------------------------------------------------------------
def test_equal_bodies_pass_including_gzip_on_either_side(capsys):
    http = FakeHttp({ID_A: (CSV_A, {}), ID_B: (_gz(CSV_B), {"content-encoding": "gzip"})})
    s3 = FakeS3({R.object_key(ID_A): (_gz(CSV_A), "gzip"), R.object_key(ID_B): (CSV_B, None)})
    rc, out, err = _run(capsys, [ID_A, ID_B], http, s3)
    assert rc == R.EXIT_OK, out + err
    assert "2 of 2 equal" in out
    assert "2026-10-31" in out and "2026-03-31" in out
    assert s3.keys == ["series/bis%3AWS_CBPOL%3AUS.csv", "series/fhfa%3Ahpi_at_3zip_quarterly%3A010.csv"]
    _no_key(out, err)


def test_a_different_body_fails_and_reports_both_newest_dates(capsys):
    stale = b"series_id,obs_date,value\nbis:WS_CBPOL:US,2026-09-30,4.25\n"
    http = FakeHttp({ID_A: (stale, {})})
    s3 = FakeS3({R.object_key(ID_A): (_gz(CSV_A), "gzip")})
    rc, out, err = _run(capsys, [ID_A], http, s3)
    assert rc == R.EXIT_DIFF
    line = [ln for ln in out.splitlines() if ln.startswith(ID_A)][0]
    assert "2026-09-30" in line and "2026-10-31" in line and "DIFFERENT" in line
    _no_key(out, err)


def test_same_newest_date_but_a_changed_value_is_still_different(capsys):
    revised = CSV_A.replace(b"4.25", b"4.50")
    rc, out, _ = _run(capsys, [ID_A], FakeHttp({ID_A: (revised, {})}), FakeS3({R.object_key(ID_A): (CSV_A, None)}))
    assert rc == R.EXIT_DIFF and "DIFFERENT" in out


def test_two_empty_sides_are_not_equal():
    assert not R.compare(b"", b"")["equal"]
    assert not R.compare(b"series_id,obs_date,value\n", b"series_id,obs_date,value\n")["equal"]
    assert R.compare(CSV_A, CSV_A)["equal"]                     # the control


def test_gzip_bytes_without_content_encoding_are_what_a_plain_client_gets(capsys):
    """Decoded only when the answer says gzip: unlabelled gzip bytes must not compare equal."""
    rc, out, _ = _run(capsys, [ID_A], FakeHttp({ID_A: (_gz(CSV_A), {})}), FakeS3({R.object_key(ID_A): (CSV_A, None)}))
    assert rc == R.EXIT_DIFF and "DIFFERENT" in out
    rc, out, _ = _run(capsys, [ID_A], FakeHttp({ID_A: (_gz(CSV_A), {"content-encoding": "gzip"})}),
                      FakeS3({R.object_key(ID_A): (CSV_A, None)}))
    assert rc == R.EXIT_OK                                      # the control


def test_one_decoder_reads_every_gzip_member_and_refuses_truncation():
    half = CSV_A.index(b"bis:", 30)
    multi = _gz(CSV_A[:half]) + _gz(CSV_A[half:])
    assert R.gunzip_bounded(multi) == CSV_A
    with pytest.raises(ValueError):
        R.gunzip_bounded(_gz(CSV_A)[:-6])
    with pytest.raises(ValueError):
        R.gunzip_bounded(_gz(CSV_A), limit=10)


def test_comment_lines_and_crlf_do_not_make_a_difference():
    served = CSV_A.replace(b"\n", b"\r\n") + b"# econdl-complete rows=2\n"
    assert R.compare(served, CSV_A)["equal"]


# ---- unfetchable ids ---------------------------------------------------------------------------------------
def test_a_served_error_is_a_failure_never_a_skip(capsys):
    http = FakeHttp({ID_A: (CSV_A, {}), ID_B: (b"{}", {})}, status={ID_B: 401})
    s3 = FakeS3({R.object_key(ID_A): (CSV_A, None), R.object_key(ID_B): (CSV_B, None)})
    rc, out, err = _run(capsys, [ID_A, ID_B], http, s3)
    assert rc == R.EXIT_DIFF
    assert "UNFETCHED (served: RuntimeError: served HTTP 401)" in out
    assert "1 of 2 equal" in out
    _no_key(out, err)


def test_a_missing_r2_object_is_a_failure_and_its_message_is_redacted(capsys):
    rc, out, err = _run(capsys, [ID_A], FakeHttp({ID_A: (CSV_A, {})}), FakeS3({}))
    assert rc == R.EXIT_DIFF and "UNFETCHED (r2:" in out
    _no_key(out, err)


# ---- refusals ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("env", [{}, {R.KEY_ENV: ""}, {R.KEY_ENV: "   "}])
def test_missing_secret_fails_with_the_owner_step_message(capsys, env):
    http = FakeHttp({ID_A: (CSV_A, {})})
    rc, out, err = _run(capsys, [ID_A], http, FakeS3({}), env=env)
    assert rc == R.EXIT_REFUSED
    assert "secret ECON_RECEIPT_API_KEY not set - owner step" in err
    assert http.calls == [], "no request is made without the secret"


def test_a_forwarding_worker_is_refused_before_any_data_request(capsys):
    http = FakeHttp({ID_A: (CSV_A, {})}, forward=True)
    s3 = FakeS3({R.object_key(ID_A): (CSV_A, None)})
    rc, out, err = _run(capsys, [ID_A], http, s3)
    assert rc == R.EXIT_REFUSED and "forward=True" in err
    assert [u for u, _ in http.calls] == [f"{BASE}/v1/edge-status"] and s3.keys == []


def test_too_many_or_malformed_ids_are_refused(capsys):
    rc, _, err = _run(capsys, [f"bis:X{i}" for i in range(R.HARD_MAX_IDS + 1)], FakeHttp({}), FakeS3({}))
    assert rc == R.EXIT_REFUSED and "at most" in err
    rc, _, err = _run(capsys, ["no-colon"], FakeHttp({}), FakeS3({}))
    assert rc == R.EXIT_REFUSED and "not a catalogue id" in err


def test_empty_ids_use_the_default_sample():
    assert R.parse_ids("") == list(R.DEFAULT_IDS)
    assert R.parse_ids("a:b, a:b c:d") == ["a:b", "c:d"]


# ---- the key never reaches output --------------------------------------------------------------------------
def test_planted_key_never_appears_even_when_an_exception_quotes_it(capsys):
    def exploding_get(url, headers):
        if url.endswith("/v1/edge-status"):
            return 200, {}, b'{"forward": false}'
        raise RuntimeError(f"boom with header {headers['X-API-Key']}")
    rc, out, err = _run(capsys, [ID_A, ID_B], exploding_get, FakeS3({}))
    assert rc == R.EXIT_DIFF
    assert "***" in out
    _no_key(out, err)


def test_planted_key_never_appears_when_the_r2_factory_fails(capsys):
    def bad_factory():
        raise RuntimeError("credentials " + PLANTED_KEY)
    rc = R.main(["--ids", ID_A, "--base", BASE], get=FakeHttp({ID_A: (CSV_A, {})}), s3_factory=bad_factory,
                environ={R.KEY_ENV: PLANTED_KEY})
    out, err = capsys.readouterr()
    assert rc == R.EXIT_DIFF and "FAILED" in err
    _no_key(out, err)


# ---- the R2 side ------------------------------------------------------------------------------------------
def test_object_key_is_the_writers_layout():
    from tools.derive_csv_bulk import csv_key
    sid = "imf_imts_direct:IMTS:USA.M.XG_FOB_USD"
    assert R.object_key(sid) == csv_key("series", "imf_imts_direct", "IMTS:USA.M.XG_FOB_USD")
    assert R.object_key("x:a(b)") == "series/x%3Aa%28b%29.csv"


def test_newest_date_reads_the_obs_date_column_and_wide_sources_are_na():
    assert R.newest_date(R.data_lines(CSV_A)) == "2026-10-31"
    assert R.newest_date(R.data_lines(b'series_id,obs_date,value\n"a,b",2025-01-01,1\n')) == "2025-01-01"
    assert R.newest_date(R.data_lines(b"country,year,gdp\nUS,2020,1\n")) == "n/a"


def test_the_production_r2_client_is_get_only(monkeypatch):
    """open_r2 over the real r2_util path, credentials faked: anything but GetObject is refused."""
    from core import r2_util
    from tools.r2_object_census import WriteRefused
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {"endpoint": "https://r2.invalid", "key": "k",
                                                              "secret": "s", "mode": "read"})
    c = R.open_r2()
    with pytest.raises(WriteRefused):
        c.put_object(Bucket=R.BUCKET, Key="series/x.csv", Body=b"")
    with pytest.raises(WriteRefused):
        c.delete_object(Bucket=R.BUCKET, Key="series/x.csv")


def test_missing_r2_credentials_refuse_clearly():
    with pytest.raises(R.Refused, match="R2 read credentials are not set"):
        R.open_r2()
