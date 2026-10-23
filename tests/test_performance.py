"""Performance helpers: Bronze tables side by side (bronze.load_all) and small-file compaction (maintain)."""

import threading
import time
import types

import pytest

pytest.importorskip("pyspark")
from lh import bronze, maintain  # noqa: E402

CFG = {"catalog": "c_dev", "code": "x", "env": "dev"}


class Spark:
    def __init__(self, tables=None):
        self.sqls, self.tables = [], tables or {}
        self.catalog = types.SimpleNamespace(tableExists=lambda t: t in self.tables)

    def sql(self, q):
        self.sqls.append(q)
        name = q.split()[-1]
        if q.startswith("OPTIMIZE"):
            self.tables[name] = (1, self.tables[name][1])
        files, size = self.tables.get(name, (0, 0))
        return types.SimpleNamespace(first=lambda: {"numFiles": files, "sizeInBytes": size})


def loader(log, fail=()):
    now, peak, lock = [0], [0], threading.Lock()

    def load(spark, cfg, table, spec, key, landing):
        with lock:
            now[0] += 1
            peak[0] = max(peak[0], now[0])
        time.sleep(0.05)
        with lock:
            now[0] -= 1
        log.append(table)
        if table in fail:
            raise ValueError(table)
        return (spec, 1, 0)

    return load, peak


def test_bronze_loads_tables_side_by_side_and_keeps_their_order():
    log, tables = [], {f"t{i}": i for i in range(8)}
    load, peak = loader(log)
    out = bronze.load_all(Spark(), CFG, tables, "k", {}, workers=4, load=load)
    assert list(out) == list(tables) and out["t5"] == (5, 1, 0)
    assert 2 <= peak[0] <= 4 and sorted(log) == sorted(tables)


def test_one_worker_is_the_old_one_after_the_other():
    log = []
    load, peak = loader(log)
    bronze.load_all(Spark(), CFG, {"a": 1, "b": 2}, "k", {}, workers=1, load=load)
    assert peak[0] == 1 and log == ["a", "b"]


def test_a_failing_table_does_not_stop_the_others_and_fails_the_task():
    log = []
    load, _ = loader(log, fail={"b"})
    with pytest.raises(RuntimeError, match="Bronze failed for"):
        bronze.load_all(Spark(), CFG, {"a": 1, "b": 2, "c": 3}, "k", {}, workers=2, load=load)
    assert sorted(log) == ["a", "b", "c"]


def test_audit_table_is_created_before_the_tables_run():
    spark, log = Spark(), []
    load, _ = loader(log)
    bronze.load_all(spark, CFG, {"a": 1}, "k", {}, workers=2, load=load)
    assert "CREATE TABLE IF NOT EXISTS c_dev.ops.audit_runs" in " ".join(spark.sqls[0].split())


def test_default_workers_come_from_config():
    log = []
    load, peak = loader(log)
    bronze.load_all(Spark(), CFG, {f"t{i}": i for i in range(6)}, "k", {}, load=load)
    assert peak[0] <= 4


def test_compaction_only_where_many_small_files():
    s = {"min_files": 50, "max_avg_mb": 8, "tables": ["ops.audit_runs", "silver.big", "ops.few", "gold.missing"]}
    spark = Spark(
        {
            "c_dev.ops.audit_runs": (60, 60 * 2000),
            "c_dev.silver.big": (60, 60 * 9 * 1048576),
            "c_dev.ops.few": (10, 1000),
        }
    )
    assert maintain.compact(spark, CFG, s) == ([("ops.audit_runs", 60, 1)], [])
    assert [q for q in spark.sqls if q.startswith("OPTIMIZE")] == ["OPTIMIZE c_dev.ops.audit_runs"]


def test_compaction_settings_list_known_tables():
    s = maintain.settings()
    assert s["min_files"] >= 2 and all(t.split(".")[0] in {"bronze", "silver", "gold", "ops"} for t in s["tables"])


def test_a_table_that_refuses_optimize_is_reported_not_raised():
    s = {"min_files": 2, "max_avg_mb": 8, "tables": ["gold.masked"]}
    spark = Spark({"c_dev.gold.masked": (9, 900)})
    real = spark.sql

    def sql(q):
        if q.startswith("OPTIMIZE"):
            raise PermissionError("not here")
        return real(q)

    spark.sql = sql
    assert maintain.compact(spark, CFG, s) == ([], [("gold.masked", "PermissionError")])
