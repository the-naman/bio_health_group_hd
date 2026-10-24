"""Access matrix (src/lh/access.py): the least-privilege rules of the design hold for both companies (no Spark)."""

import json

import pytest

from lh import access
from tests.conftest import ROOT

ENVS = ("dev", "test", "prod")
CODES = ("bhg", "fqf")


def privs(code, env, who, scope="catalog"):
    return {p for n, s, ps in access.matrix(code, env) if n == who and s == scope for p in ps}


@pytest.mark.parametrize("code", CODES)
def test_dev_is_a_shared_sandbox_for_the_three_dev_roles(code):
    for role in ("lh_developer", "lh_teamlead", "lh_manager"):
        assert {"MODIFY", "READ VOLUME", "WRITE VOLUME", "MANAGE"} <= privs(code, "dev", role)


@pytest.mark.parametrize(("code", "env"), [(c, e) for c in CODES for e in ("test", "prod")])
def test_only_the_job_account_writes_and_reads_raw_files_in_test_and_prod(code, env):
    writers = {n for n, s, ps in access.matrix(code, env) if "MODIFY" in ps or "WRITE VOLUME" in ps}
    assert writers == {f"sp_{code}_{env}"}
    for role in ("lh_developer", "lh_teamlead", "lh_manager"):
        assert not privs(code, env, role) & {"READ VOLUME", "WRITE VOLUME", "MODIFY"}
    assert (env == "test") == ("MANAGE" in privs(code, env, "lh_manager"))


@pytest.mark.parametrize(("code", "env"), [(c, e) for c in CODES for e in ENVS])
def test_management_reads_and_manages_but_never_writes(code, env):
    p = privs(code, env, "lh_management")
    assert {"MANAGE", "READ VOLUME", "SELECT"} <= p and not p & {"MODIFY", "WRITE VOLUME"}


@pytest.mark.parametrize(("code", "env"), [(c, e) for c in CODES for e in ENVS])
def test_business_manager_reads_gold_only(code, env):
    rows = [(s, ps) for n, s, ps in access.matrix(code, env) if n == "lh_bizmanager"]
    assert rows == ([] if env == "dev" else [("gold", access.GOLD_R)])


def test_group_view_reads_fqf_gold_in_test_and_prod_only():
    for env in ENVS:
        rows = [(s, ps) for n, s, ps in access.matrix("fqf", env) if n == f"sp_bhg_{env}"]
        assert rows == ([] if env == "dev" else [("gold", access.GOLD_R)])
    assert not [n for e in ENVS for n, _, _ in access.matrix("bhg", e) if n.startswith("sp_fqf")]


@pytest.mark.parametrize("code", CODES)
def test_refresh_only_flows_down_and_only_into_refresh_in(code):
    order = {e: i for i, e in enumerate(ENVS)}
    for src, tgt in access.REFRESH:
        assert order[src] > order[tgt]
    for env in ENVS:
        for who, (kind, obj), ps in access.refresh_grants(code, env):
            assert who.startswith(f"sp_{code}_") and obj.startswith(f"{code}_{env}")
            allowed = [("CATALOG", ["USE CATALOG"]), ("SCHEMA", ["USE SCHEMA"]), ("VOLUME", access.REFRESH_VOLUME)]
            assert (kind, ps) in allowed
            if kind == "VOLUME":
                assert obj.endswith(".ops.refresh_in")


def test_grant_and_verify_notebooks_use_the_one_matrix():
    for nb in ("01_grant_access", "02_verify_setup"):
        cells = json.loads((ROOT / "setup_once" / "notebook" / f"{nb}.ipynb").read_text())["cells"]
        src = "".join("".join(c["source"]) for c in cells)
        assert "from lh import access" in src and '"lh_developer", "catalog"' not in src, nb
