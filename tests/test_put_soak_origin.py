"""tools/selfhost/put_soak_origin.py: the soak worker's two origin values are set from files, on wrangler's
standard input, on the soak worker only - and never shown.

No wrangler runs here: `run` is replaced by a recorder. Each refusal has the passing call beside it.
"""
import os
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
    args = ["--url-file", str(url), "--dev-vars", str(dev), "--worker-dir", str(worker)]
    return types.SimpleNamespace(worker=worker, url=url, dev=dev, args=args)


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
