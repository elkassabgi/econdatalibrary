"""stat_slovenia: an ALL-NULL boundary body is 'nothing published yet', not a break.

Reproduced live 2026-08-06 on 2221405S: SURS pre-lists the next period ('2024') in
LETO's codelist before publishing data — the boundary POST returns a real json-stat2
structure with 36 values, every one null. bool([None]*36) is True, so the unpopulated
forward period was classified a STRUCTURAL break on every sweep.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers.stat_slovenia import _body_has_data  # noqa: E402


def test_all_null_body_is_not_data():
    assert _body_has_data({"value": [None] * 36}) is False


def test_empty_and_missing_bodies_are_not_data():
    assert _body_has_data({"value": []}) is False
    assert _body_has_data({}) is False
    assert _body_has_data(None) is False


def test_any_real_value_is_data():
    assert _body_has_data({"value": [None, None, 28552]}) is True
