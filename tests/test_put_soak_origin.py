"""tools/selfhost/put_soak_origin.py: the soak worker's two origin values are set from files, on wrangler's
standard input, on the soak worker only - and never shown.

No wrangler runs here: `run` is replaced by a recorder, and the soak address is a stand-in (`Soak`): nothing
goes to the network. Each refusal has the passing call beside it.
"""
import http.server
import json
import os
import threading
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))

import put_soak_origin as P  # noqa: E402

SECRET = "0123456789abcdef" * 4
URL = "https://econ-origin-test.econdatalibrary.com"


class Recorder:
    def __init__(self, codes=(0, 0), echo=False):
        self.calls, self.codes, self.echo = [], list(codes), echo

    def __call__(self, argv, input=None, cwd=None, capture_output=None, text=None):   # noqa: A002
        self.calls.append({"argv": list(argv), "input": input, "cwd": cwd})
        out = f"Success! Uploaded secret {argv[4]}" + (f" value {input.decode().strip()}" if self.echo else "")
        return types.SimpleNamespace(returncode=self.codes[len(self.calls) - 1], stdout=out, stderr="")


COMMIT = "c0ffee00" * 5


def _status(commit=COMMIT, soak=True, forward=True, state="users", configured=False):
    return {"commit": commit, "soak": soak, "forward": forward, "edge_state": state, "forward_raw": "on",
            "edge_state_raw": "users", "origin_configured": configured}


class Soak:
    """The soak address. `answers` are given one per read; the last one is repeated. The default is what a
    deployed soak worker answers: no origin values at the first read, both set from the second read on."""

    def __init__(self, *answers):
        self.answers = list(answers) or [_status(), _status(configured=True)]
        self.reads = []

    def __call__(self, soak):
        self.reads.append(soak)
        return self.answers[min(len(self.reads), len(self.answers)) - 1]


@pytest.fixture
def rig(tmp_path, monkeypatch):
    worker = tmp_path / "worker"
    worker.mkdir()
    (worker / "wrangler.soak.toml").write_text('name = "econdl-api-soak"\n', encoding="utf-8")
    url = tmp_path / "origin_url.txt"
    url.write_text(URL + "\n", encoding="utf-8")
    dev = tmp_path / ".dev.vars"
    dev.write_text(f"ORIGIN_SECRET={SECRET}\n", encoding="utf-8")
    monkeypatch.setattr(P.shutil, "which", lambda name: "/fake/bin/" + name)
    soak = Soak()
    monkeypatch.setattr(P, "fetch_status", soak)                # NO test of this file may read the real address
    monkeypatch.delenv("SELFHOST_SOAK", raising=False)
    args = ["--url-file", str(url), "--dev-vars", str(dev), "--worker-dir", str(worker)]
    return types.SimpleNamespace(worker=worker, url=url, dev=dev, args=args, soak=soak)


def test_both_values_go_to_the_soak_worker_on_stdin_and_are_never_shown(rig, capsys):
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 0
    assert [c["argv"] for c in rec.calls] == [
        ["/fake/bin/npx", "wrangler", "secret", "put", "ORIGIN_URL", "--config", "wrangler.soak.toml", "--name", "econdl-api-soak"],
        ["/fake/bin/npx", "wrangler", "secret", "put", "ORIGIN_SECRET", "--config", "wrangler.soak.toml", "--name", "econdl-api-soak"]]
    assert [c["input"] for c in rec.calls] == [(URL + "\n").encode(), (SECRET + "\n").encode()]
    assert all(c["cwd"] == str(rig.worker) for c in rec.calls)
    shown = "".join(capsys.readouterr())
    assert SECRET not in shown and URL not in shown
    assert "ORIGIN_URL: wrangler exit code 0" in shown and "ORIGIN_SECRET: wrangler exit code 0" in shown


def test_output_of_wrangler_that_holds_a_value_is_not_shown(rig, capsys):
    assert P.main(rig.args, run=Recorder(echo=True)) == 0
    shown = "".join(capsys.readouterr())
    assert SECRET not in shown and URL not in shown and shown.count("is not shown") == 2


@pytest.mark.parametrize("url", [
    "https://econ-origin.econdatalibrary.org", "https://econdatalibrary.com.evil.example",
    "https://a.b.econdatalibrary.com", "http://econ-origin.econdatalibrary.com",
    "https://econ-origin.econdatalibrary.com/", "https://econ-origin.econdatalibrary.com:8443",
    "https://econ-origin.econdatalibrary.com/x", "https://evil.example/#.econdatalibrary.com",
    "https://econ-origin.econdatalibrary.com@evil.example", "https://ECON-ORIGIN.econdatalibrary.com",
    "https://-x.econdatalibrary.com", "https://econdatalibrary.com", "", URL + "\n" + URL,
    "https://xecondatalibrary.com", "https://evil-econdatalibrary.com", "https://a.notecondatalibrary.com",
    "https://a.econdatalibraryxcom", "https://axecondatalibrary.com", "https://a.econdatalibrary.com.",
    "https://a.econdatalibrary.co", "https://a.econdatalibrary.com.evil.example",
])
def test_an_address_outside_the_zone_or_with_anything_after_the_host_is_refused(rig, url, capsys):
    rig.url.write_text(url + "\n", encoding="utf-8")
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 1
    assert rec.calls == [], "nothing was sent"
    shown = "".join(capsys.readouterr())
    assert "refused" in shown
    assert all(part not in shown for part in url.split() if part), "a refused address is not printed either"


def test_wranglers_error_lines_are_shown_and_a_value_on_its_error_stream_is_not(rig, capsys):
    """wrangler writes its errors on the error stream: they must reach the owner, and a value there must not."""
    def failing(argv, input=None, cwd=None, capture_output=None):   # noqa: A002
        return types.SimpleNamespace(returncode=1, stdout=b"", stderr="\u2718 [ERROR] request failed [code: 10000]\n".encode())
    assert P.main(rig.args, run=failing) == 1
    assert "[ERROR] request failed [code: 10000]" in capsys.readouterr().out

    def echoing(argv, input=None, cwd=None, capture_output=None):   # noqa: A002
        return types.SimpleNamespace(returncode=0, stdout=b"ok\n", stderr=b"debug: " + input)
    assert P.main(rig.args, run=echoing) == 0
    shown = "".join(capsys.readouterr())
    assert SECRET not in shown and URL not in shown and shown.count("is not shown") == 2


@pytest.mark.parametrize("body", [
    "", "ORIGIN_SECRET=\n", "ORIGIN_SECRET=short\n", f"ORIGIN_SECRET={SECRET.upper()}\n",
    f"ORIGIN_SECRET={SECRET}0\n", f"ORIGIN_SECRET={SECRET}\nORIGIN_SECRET={SECRET}\n", f"OTHER={SECRET}\n",
    f"# ORIGIN_SECRET={SECRET}\n",
])
def test_a_dev_vars_file_without_exactly_one_good_secret_is_refused(rig, body, capsys):
    rig.dev.write_text(body, encoding="utf-8")
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 1
    assert rec.calls == []
    err = capsys.readouterr().err
    assert "refused" in err and SECRET not in err and SECRET.upper() not in err


def test_other_lines_and_quotes_in_dev_vars_are_fine(rig):
    rig.dev.write_text(f'# the origin\nOTHER=1\nORIGIN_SECRET="{SECRET}"\n', encoding="utf-8")
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 0 and rec.calls[1]["input"] == (SECRET + "\n").encode()


def test_without_the_soak_config_nothing_is_sent(rig, capsys):
    os.remove(rig.worker / "wrangler.soak.toml")
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 1 and rec.calls == []
    assert "wrangler.soak.toml" in capsys.readouterr().err


def test_when_the_address_is_not_set_the_secret_is_not_sent(rig):
    rec = Recorder(codes=(1, 0))
    assert P.main(rig.args, run=rec) == 1
    assert [c["argv"][4] for c in rec.calls] == ["ORIGIN_URL"]


def test_a_failed_secret_is_a_failure(rig):
    assert P.main(rig.args, run=Recorder(codes=(0, 1))) == 1


def test_a_missing_file_or_npx_is_a_refusal_not_a_traceback(rig, monkeypatch, capsys):
    os.remove(rig.dev)
    assert P.main(rig.args, run=Recorder()) == 1
    rig.dev.write_text(f"ORIGIN_SECRET={SECRET}\n", encoding="utf-8")
    monkeypatch.setattr(P.shutil, "which", lambda name: None)
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 1 and rec.calls == []
    assert "npx is not on PATH" in capsys.readouterr().err


def test_with_the_real_subprocess_and_wranglers_own_bytes_the_receipt_is_whole(rig, monkeypatch, capfd, tmp_path):
    """AR-274: the recorder above never showed what the REAL subprocess.run does with wrangler's output. A
    stand-in (this interpreter, no wrangler) prints wrangler 3.114.17's banner bytes; nothing may be lost and
    no traceback may appear, whatever the code page."""
    standin = tmp_path / "standin.py"
    standin.write_text(
        "import sys\n"
        "data = sys.stdin.buffer.read()\n"
        "open(sys.argv[-1], 'ab').write(data)\n"
        "sys.stdout.buffer.write(' \\u26c5\\ufe0f wrangler 3.114.17\\n? There does not seem to be a Worker\\n'"
        ".encode('utf-8') + ('\\u2728 Success! Uploaded secret ' + sys.argv[4] +'\\n').encode('utf-8'))\n",
        encoding="utf-8")
    seen = tmp_path / "stdin.bin"
    monkeypatch.setattr(P, "command", lambda name: [sys.executable, "-B", str(standin), "wrangler", "secret", "put", name, str(seen)])
    assert P.main(rig.args) == 0
    out, err = capfd.readouterr()
    assert "Traceback" not in err and "UnicodeDecodeError" not in err, err[-300:]
    assert out.count("There does not seem to be a Worker") == 2 and out.count("Success! Uploaded secret") == 2
    assert seen.read_bytes() == (URL + "\n" + SECRET + "\n").encode(), "the value and one LF, no CR"
    assert SECRET not in out + err and URL not in out + err


@pytest.mark.parametrize("raw, ok", [
    (b"\xef\xbb\xbf" + URL.encode() + b"\r\n", True),                       # a byte-order mark is not part of the address
    ((URL + "\r\n").encode("utf-16"), False),                                # PowerShell 5's `>`
])
def test_a_file_from_notepad_or_powershell_is_read_or_refused_never_a_traceback(rig, raw, ok, capsys):
    rig.url.write_bytes(raw)
    rec = Recorder()
    assert P.main(rig.args, run=rec) == (0 if ok else 1)
    assert len(rec.calls) == (2 if ok else 0)
    if not ok:
        assert "not UTF-8" in capsys.readouterr().err


def test_the_name_and_the_config_are_the_soak_workers():
    import tomllib
    with open(os.path.join(ROOT, "api", "worker", P.SOAK_CONFIG), "rb") as fh:
        assert tomllib.load(fh)["name"] == P.SOAK_NAME == "econdl-api-soak"


# ---------------------------------------------------------------- the soak address, before and after


NOT_DEPLOYED = {
    "no worker at the address": "notjson", "a 404": "http-404", "no answer": "unreachable-URLError",
    "a JSON list": [1], "an empty object": {},
    "a worker that is not a soak worker": _status(soak=False), "the soak flag as a string": _status(soak="yes"),
    "a worker without a commit": _status(commit=None), "a short commit": _status(commit="c0ffee0"),
    "a soak worker that does not forward": _status(forward=False),
    "a soak worker whose state is not users": _status(state="econ"),
}


@pytest.mark.parametrize("what", sorted(NOT_DEPLOYED))
def test_before_the_deploy_nothing_is_sent(rig, what, capsys):
    """AR-274 B4: `wrangler secret put` on a worker that does not exist creates it (it is not asked: its standard
    input is a pipe). The address must first answer as deploy_soak.sh leaves it. The control is every test above."""
    rec, soak = Recorder(), Soak(NOT_DEPLOYED[what])
    assert P.main(rig.args, run=rec, fetch=soak) == 1
    assert rec.calls == [] and len(soak.reads) == 1
    out, err = capsys.readouterr()
    assert "Run deploy_soak.sh first" in err and "Nothing was sent" in err
    assert SECRET not in out + err and URL not in out + err


def test_the_address_read_is_the_soak_workers_and_can_be_named(rig):
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 0
    assert set(rig.soak.reads) == {"https://econdl-api-soak.elkassabgi.workers.dev"} and len(rig.soak.reads) == 2
    soak = Soak()
    assert P.main(rig.args + ["--soak", "https://soak.example"], run=Recorder(), fetch=soak) == 0
    assert set(soak.reads) == {"https://soak.example"}


def test_check_reads_the_files_and_the_address_and_sends_nothing(rig, capsys):
    rec = Recorder()
    assert P.main(rig.args + ["--check"], run=rec) == 0
    assert rec.calls == [] and len(rig.soak.reads) == 1
    out, err = capsys.readouterr()
    assert "Nothing was sent" in out and "origin_configured is not true" in out and COMMIT in out
    assert SECRET not in out + err and URL not in out + err
    rec, soak = Recorder(), Soak("notjson")                                  # --check before the deploy: not a pass
    assert P.main(rig.args + ["--check"], run=rec, fetch=soak) == 1 and rec.calls == []
    rig.url.write_text("https://econ-origin-test.econdatalibrary.org\n", encoding="utf-8")     # --check with a bad file
    rec, soak = Recorder(), Soak()
    assert P.main(rig.args + ["--check"], run=rec, fetch=soak) == 1 and rec.calls == [] and soak.reads == []


def test_values_that_were_set_and_do_not_show_are_not_a_success(rig, capsys):
    """wrangler ended with 0 twice; the address never says origin_configured true."""
    rec, soak, waits = Recorder(), Soak(_status()), []
    assert P.main(rig.args, run=rec, fetch=soak, sleep=waits.append) == 1
    assert len(rec.calls) == 2 and len(soak.reads) == 1 + P.TRIES_AFTER and waits == [P.WAIT_AFTER] * (P.TRIES_AFTER - 1)
    out, err = capsys.readouterr()
    assert "NOT SHOWN" in err and "seen after both values" not in out
    assert SECRET not in out + err and URL not in out + err


def test_values_that_show_on_a_later_answer_are_a_success(rig, capsys):
    rec, soak, waits = Recorder(), Soak(_status(), _status(), "unreachable-TimeoutError", _status(configured=True)), []
    assert P.main(rig.args, run=rec, fetch=soak, sleep=waits.append) == 0
    assert len(soak.reads) == 4 and waits == [P.WAIT_AFTER] * 2
    out = capsys.readouterr().out
    assert "seen after both values" in out and "(before this run it was not true)" in out


def test_another_worker_at_the_address_after_the_values_is_not_a_success(rig, capsys):
    """The answer after must be the SAME four words as before: a deploy that landed in between is not this run's proof."""
    rec, soak = Recorder(), Soak(_status(), _status(commit="0ddba11" + "0" * 33, configured=True))
    assert P.main(rig.args, run=rec, fetch=soak, sleep=lambda s: None) == 1
    assert "NOT SHOWN" in capsys.readouterr().err


def test_values_that_were_set_before_this_run_are_said_to_show_no_change(rig, capsys):
    rec, soak = Recorder(), Soak(_status(configured=True))
    assert P.main(rig.args, run=rec, fetch=soak) == 0 and len(rec.calls) == 2
    assert "(before this run it was true)" in capsys.readouterr().out


def test_a_failed_put_reads_the_address_once_only(rig):
    for codes in ((1, 0), (0, 1)):
        rec, soak = Recorder(codes=codes), Soak()
        assert P.main(rig.args, run=rec, fetch=soak) == 1 and len(soak.reads) == 1


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):                                                         # noqa: N802
        self.server.seen.append((self.path, self.headers.get("User-Agent")))
        code, body = self.server.answer
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def test_the_real_reader_against_a_local_listener(monkeypatch):
    """`fetch_status` itself (the fixture replaces it everywhere else): a listener on a free loopback port."""
    monkeypatch.undo()
    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    srv.seen = []
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        srv.answer = (200, json.dumps(_status(configured=True)).encode())
        assert P.soak_status(base, P.fetch_status) == (COMMIT + " yes yes users", True)
        assert srv.seen == [("/v1/edge-status", P.UA)]
        srv.answer = (200, b"<html>There is nothing here yet</html>")
        assert P.soak_status(base, P.fetch_status) == ("notjson", False)
        srv.answer = (404, b"{}")
        assert P.soak_status(base, P.fetch_status) == ("http-404", False)
        srv.answer = (200, b"[]")
        assert P.soak_status(base, P.fetch_status) == ("notjson", False)
        srv.answer = (200, json.dumps(_status(configured="true")).encode())       # the string is not the boolean
        assert P.soak_status(base, P.fetch_status) == (COMMIT + " yes yes users", False)
    finally:
        srv.shutdown()
        srv.server_close()
    line, configured = P.soak_status(base, P.fetch_status)                        # the port is closed now
    assert line.startswith("unreachable-") and configured is False


def test_an_answer_of_another_version_is_tried_again(rig, capsys):
    """One answer after the values comes from another commit (a deploy that spreads); the next one is this
    worker's with origin_configured true. The loop must go on to it, not end at the first answer that is set."""
    other = _status(commit="0ddba11" + "0" * 33, configured=True)
    rec, soak, waits = Recorder(), Soak(_status(), other, _status(configured=True)), []
    assert P.main(rig.args, run=rec, fetch=soak, sleep=waits.append) == 0
    assert len(soak.reads) == 3 and waits == [P.WAIT_AFTER]


def test_the_address_can_come_from_the_same_variable_as_in_the_shell_scripts(rig, monkeypatch):
    monkeypatch.setenv("SELFHOST_SOAK", "https://soak.example")
    assert P.main(rig.args, run=Recorder()) == 0
    assert set(rig.soak.reads) == {"https://soak.example"}
