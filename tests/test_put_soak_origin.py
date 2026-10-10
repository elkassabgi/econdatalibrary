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
        out = f"Success! Uploaded secret {argv[4]}" + (f" value {input.strip()}" if self.echo else "")
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
    assert [c["input"] for c in rec.calls] == [URL + "\n", SECRET + "\n"]
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
])
def test_an_address_outside_the_zone_or_with_anything_after_the_host_is_refused(rig, url, capsys):
    rig.url.write_text(url + "\n", encoding="utf-8")
    rec = Recorder()
    assert P.main(rig.args, run=rec) == 1
    assert rec.calls == [], "nothing was sent"
    assert "refused" in capsys.readouterr().err


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
    assert P.main(rig.args, run=rec) == 0 and rec.calls[1]["input"] == SECRET + "\n"


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


def test_the_name_and_the_config_are_the_soak_workers():
    import tomllib
    with open(os.path.join(ROOT, "api", "worker", P.SOAK_CONFIG), "rb") as fh:
        assert tomllib.load(fh)["name"] == P.SOAK_NAME == "econdl-api-soak"
