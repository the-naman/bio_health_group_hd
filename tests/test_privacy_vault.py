"""Keyed token masks (ADR 0010): the token key and function live in the vault in test/prod, never in dev (no Spark)."""

import re

import pytest

from lh import access, privacy

CFG = {"catalog": "bhg_test", "code": "bhg"}
SP = "0f8fad5b-d9cb-469f-a165-70867728950e"  # a made-up job account id (UUID shape only)


class Rows(list):
    def collect(self):
        return list(self)

    def first(self):
        return self[0]


class FakeSpark:
    """Records SQL text; answers the few information_schema questions privacy.py asks."""

    def __init__(self, masks=None, routines=(), tables=None, key_exists=False):
        self.sqls, self.saved, self.key_exists = [], [], key_exists
        self.masks = dict(masks or {})  # (schema, table, column) -> full function name
        self.routines, self.tables = list(routines), tables or {}
        spark = self

        class Catalog:
            def tableExists(self, name):  # noqa: N802 (Spark API name)
                return spark.key_exists

        class Writer:
            def __init__(self, rows):
                self.rows = rows

            def mode(self, m):
                assert m == "errorifexists"
                return self

            def option(self, *a):
                return self

            def saveAsTable(self, name):  # noqa: N802
                spark.saved.append((name, self.rows))
                spark.key_exists = True

        class DF:
            def __init__(self, rows):
                self.write = Writer(rows)

        self.catalog = Catalog()
        self.createDataFrame = lambda rows, schema: DF(rows)  # noqa: N815

    def sql(self, q):
        self.sqls.append(q)
        if "session_user()" in q and q.strip().startswith("SELECT"):
            return Rows([(SP,)])
        if "information_schema.routines" in q:
            return Rows([(r,) for r in self.routines])
        if "information_schema.column_masks" in q:
            return Rows([(*k, v) for k, v in self.masks.items()])
        if "information_schema.columns" in q:
            layer = re.search(r"table_schema = '(\w+)'", q).group(1)
            return Rows([(t, c, d) for (lay, t, c), d in self.tables.items() if lay == layer])
        if "information_schema.tables" in q or "row_filters" in q:
            return Rows([])
        d = re.match(r"DROP FUNCTION IF EXISTS \w+\.ops\.(\w+)$", q)
        if d and d.group(1) in self.routines:
            self.routines.remove(d.group(1))
        m = re.match(r"\s*ALTER TABLE \S+\.(\w+)\.(\w+) ALTER COLUMN `(\w+)` (SET MASK (\S+)|DROP MASK)", q)
        if m:
            key = (m.group(1), m.group(2), m.group(3))
            self.masks.pop(key, None) if m.group(5) is None else self.masks.__setitem__(key, m.group(5))
        return Rows([])


@pytest.fixture
def small(monkeypatch):
    """One gold table with one token column and one hidden column, so apply() can run end to end."""
    p = {
        "clear": {"dev": ["lh_developer"], "test": ["lh_management"], "prod": ["lh_management"]},
        "pipeline": {"test": "sp_bhg_test", "prod": "sp_bhg_prod"},
        "vault": {"catalog": "lh_vault", "environments": ["test", "prod"]},
        "layers": ["gold"],
        "personal": {"dim_x": {"fp": "token", "phone": "hide"}},
        "technical": [],
        "ids": {},
    }
    monkeypatch.setattr(privacy, "load", lambda: p)
    monkeypatch.setattr(privacy, "bronze_names", lambda: {})
    return {("gold", "dim_x", "fp"): "string", ("gold", "dim_x", "phone"): "string"}


def test_config_matches_the_access_matrix():
    v = privacy.load()["vault"]
    assert v["catalog"] == access.VAULT
    assert tuple(v["environments"]) == access.VAULT_ENVS == ("test", "prod")


@pytest.mark.parametrize("env", ["test", "prod"])
def test_token_lives_in_the_vault_in_test_and_prod(env):
    cfg = {"catalog": f"bhg_{env}"}
    assert privacy.function_name(cfg, "token", env=env) == f"lh_vault.bhg_{env}.mask_token"
    assert privacy.function_name(cfg, "last4", env=env) == f"bhg_{env}.ops.mask_last4"  # other masks stay in ops


def test_dev_keeps_the_plain_token_in_ops():
    assert privacy.vault("dev") is None
    assert privacy.function_name({"catalog": "bhg_dev"}, "token", env="dev") == "bhg_dev.ops.mask_token"
    assert privacy.function_name({"catalog": "bhg_dev"}, "token") == "bhg_dev.ops.mask_token"  # old call style


def test_keyed_token_reads_the_key_and_never_contains_it(small):
    spark = FakeSpark()
    name = privacy.create_function(spark, CFG, "test", "token")
    assert name == "lh_vault.bhg_test.mask_token"
    [(table, rows)] = spark.saved
    assert table == "lh_vault.bhg_test.mask_key"
    key = rows[0][0]
    assert re.fullmatch(r"[0-9a-f]{64}", key)
    body = spark.sqls[-1]
    assert "SELECT max(k) FROM lh_vault.bhg_test.mask_key" in body and "sha2(concat(" in body
    assert all(key not in q for q in spark.sqls)  # the key is written as data, never as SQL text


def test_key_is_made_once_and_never_replaced(small):
    spark = FakeSpark(key_exists=True)
    privacy.create_function(spark, CFG, "test", "token")
    assert spark.saved == []
    assert privacy.ensure_key(spark, CFG, "test") is False


def test_dev_token_is_unkeyed(small):
    spark = FakeSpark()
    privacy.create_function(spark, {"catalog": "bhg_dev"}, "dev", "token")
    assert spark.saved == [] and "mask_key" not in spark.sqls[-1] and "bhg_dev.ops.mask_token" in spark.sqls[-1]


def test_apply_moves_token_columns_to_the_vault_and_drops_the_plain_function(small):
    old = {
        ("gold", "dim_x", "fp"): "bhg_test.ops.mask_token",
        ("gold", "dim_x", "phone"): "bhg_test.ops.mask_hide_string",
    }
    spark = FakeSpark(masks=old, routines=["mask_token", "mask_hide_string"], tables=small)
    done = privacy.apply(spark, CFG, "test")
    assert spark.masks[("gold", "dim_x", "fp")] == "lh_vault.bhg_test.mask_token"
    assert spark.masks[("gold", "dim_x", "phone")] == "bhg_test.ops.mask_hide_string"
    assert done["set"] == 1 and done["kept"] == 1 and done["unkeyed_token_dropped"] == 1
    assert "DROP FUNCTION IF EXISTS bhg_test.ops.mask_token" in spark.sqls
    # second run: nothing to move, nothing to drop, same key
    again = privacy.apply(spark, CFG, "test")
    assert again["set"] == 0 and "unkeyed_token_dropped" not in again and len(spark.saved) == 1


def test_apply_in_dev_changes_nothing_about_tokens(small):
    old = {
        ("gold", "dim_x", "fp"): "bhg_dev.ops.mask_token",
        ("gold", "dim_x", "phone"): "bhg_dev.ops.mask_hide_string",
    }
    spark = FakeSpark(masks=old, routines=["mask_token", "mask_hide_string"], tables=small)
    done = privacy.apply(spark, {"catalog": "bhg_dev"}, "dev")
    assert done["set"] == 0 and done["kept"] == 2 and spark.saved == []
    assert not any(q.startswith("DROP FUNCTION") for q in spark.sqls)


def test_vault_access_is_only_each_job_account_on_its_own_schema():
    grants = access.vault_grants()
    who = {p for p, _, _ in grants}
    assert who == {f"sp_{c}_{e}" for c in ("bhg", "fqf") for e in ("test", "prod")}
    for p, (kind, name), privs in grants:
        if kind == "SCHEMA":
            assert name == f"lh_vault.{p[3:]}"  # sp_bhg_test -> lh_vault.bhg_test, nobody else's schema
            assert set(privs) == {"USE SCHEMA", "CREATE TABLE", "CREATE FUNCTION"}
        else:
            assert (kind, name, privs) == ("CATALOG", "lh_vault", ["USE CATALOG"])
    assert not any(p.startswith("lh_") for p in who)  # no person group, no CI account
