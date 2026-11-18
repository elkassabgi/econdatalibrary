"""The daily digest shows what gus_dbw has rebuilt but nobody has published (PR #291, R1937).

The crawler only RECORDS an owed publish in _publish_owed.jsonl and never publishes. Without a line
in the email, a passed rebuild would sit unserved with nothing saying so. The same line names the
areas the row-loss guard is holding back, because a refused area stays due and a persistent cause
refuses it on every pass.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import updater.send_digest as sd                   # noqa: E402
from jobs import gus_dbw_refresh as R              # noqa: E402

NOW = dt.datetime(2026, 11, 18, 12, 0, 0, tzinfo=dt.timezone.utc)


def _owe(d, area, md5, hours_ago):
    with open(os.path.join(d, R.OWED_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps({"area": area, "md5": md5,
                            "rebuilt_utc": (NOW - dt.timedelta(hours=hours_ago)).isoformat()}) + "\n")


def test_owed_count_and_oldest_age(tmp_path):
    d = str(tmp_path)
    _owe(d, "7", "aa", 30.0)        # published below: NOT owed
    _owe(d, "9", "bb", 5.0)
    _owe(d, "43", "cc", 2.0)
    with open(os.path.join(d, R.PUBLISHED_FILE), "w", encoding="utf-8") as f:
        f.write(json.dumps({"area": "7", "md5": "aa"}) + "\n")
    lines = sd.gus_dbw_publish_lines(d, NOW)
    assert lines == ["     gus_dbw publish: 2 area(s) rebuilt and OWED to the publisher, "
                     "oldest rebuilt 5.0h ago (area_9)"]


def test_nothing_owed_is_said(tmp_path):
    assert sd.gus_dbw_publish_lines(str(tmp_path), NOW) == ["     gus_dbw publish: 0 areas owed"]


def test_refused_areas_and_torn_lines_are_attention(tmp_path):
    d = str(tmp_path)
    os.makedirs(os.path.join(d, R.REFUSED_SUBDIR, "area_5", "20261118T110000Z"))
    with open(os.path.join(d, R.OWED_FILE), "w", encoding="utf-8") as f:
        f.write('{"area": "8", "md5": "ab')                       # torn last line
    lines = sd.gus_dbw_publish_lines(d, NOW)
    assert any("ATTENTION 1 unreadable line" in ln for ln in lines)
    assert any(ln.startswith("     gus_dbw guard: ATTENTION 1 area(s) refused and still due, newest "
                             "refusal 1.0h ago (area_5)") for ln in lines)


def test_an_unreadable_record_is_said_not_silent(tmp_path):
    d = str(tmp_path)
    os.makedirs(os.path.join(d, R.OWED_FILE))                     # a directory where the file should be
    lines = sd.gus_dbw_publish_lines(d, NOW)
    assert len(lines) == 1 and "could not be read" in lines[0] and "ATTENTION" in lines[0]


def test_the_line_reaches_both_bodies():
    src = open(sd.__file__, encoding="utf-8").read()
    cut = src.index("# ---- HTML (the real email) ----")
    assert "lines += _gpub" in src[:cut], "the text body stopped showing gus_dbw's owed publishes"
    assert "for l in _gpub" in src[cut:], "the html body stopped showing gus_dbw's owed publishes"
