"""Data dictionary (config/dictionary.yml): every Silver table, Gold table and BI view is described (no Spark)."""

import pytest
import yaml

from lh import bi, dictionary
from tests.conftest import CODE, ROOT, STREAM_COMPANIES


def cfg(name):
    return yaml.safe_load((ROOT / "config" / f"{name}.yml").read_text())


def test_dictionary_loads_and_every_text_is_one_safe_line():
    d = dictionary.load()
    assert set(d) == {"silver", "gold", "views"}


@pytest.mark.parametrize("bad", ["it's", "two\nlines", "x", "a\\b", None])
def test_unsafe_description_is_refused(tmp_path, bad):
    p = tmp_path / "dictionary.yml"
    p.write_text(yaml.safe_dump({"silver": {"t": {"about": bad}}}))
    with pytest.raises(ValueError):
        dictionary.load(str(p))


def test_every_silver_table_is_described():
    assert set(cfg("silver")["tables"]) <= set(dictionary.load()["silver"])


def test_every_gold_table_is_described_and_every_described_table_is_built():
    built = [n for n in cfg("gold")["sql_build"][CODE] if not n.startswith("_")]
    if CODE == "bhg":
        built += [n.split("/")[1] for n in cfg("group")["sql_build"] if not n.split("/")[1].startswith("_")]
    if CODE in STREAM_COMPANIES:
        built.append("payment_failure_windows")
    gold = dictionary.load()["gold"]
    assert set(gold) == set(built)
    assert all(spec.get("columns") for spec in gold.values())


def test_every_bi_view_is_described():
    assert set(dictionary.load()["views"]) == set(bi.wanted({"code": CODE}))
