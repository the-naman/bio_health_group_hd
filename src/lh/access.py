"""Access matrix (same file in both repos): which group or job account gets which Unity Catalog privileges.

One source for setup_once/notebook/01_grant_access (grants it) and 02_verify_setup (checks it), so the two can never
disagree. Scope "catalog" = on the catalog; a schema name ("gold", "ops") = on that schema only (plus USE CATALOG
and BROWSE on the catalog). Raw source files (READ/WRITE VOLUME) only where real values may be seen: in dev the roles
that run the pipeline, in test and prod Management and the job account; everyone else reads masked tables.
"""

FILES = ["READ VOLUME", "WRITE VOLUME"]  # a column mask cannot protect a file
R_DATA = ["USE CATALOG", "USE SCHEMA", "SELECT", "EXECUTE", "BROWSE"]  # tables only (masked), no raw files
R = R_DATA + ["READ VOLUME"]
RW = R + ["MODIFY", "CREATE TABLE", "CREATE VOLUME", "WRITE VOLUME", "CREATE FUNCTION", "CREATE MATERIALIZED VIEW"]
MANAGE = ["MANAGE"]
GOLD_R = ["USE SCHEMA", "SELECT", "EXECUTE"]  # schema gold only
CI_GOLD = ["USE SCHEMA", "SELECT"]  # lh_ci (sp_lh_notices): notices and alerts read Gold views
CI_OPS = ["USE SCHEMA", "SELECT", "CREATE TABLE"]  # and the run logs; it owns ops.notice_log
REFRESH = [("prod", "test"), ("test", "dev")]  # 5.11 controlled refresh: source env -> target env
REFRESH_VOLUME = ["READ VOLUME", "WRITE VOLUME"]  # source job account on <code>_<target>.ops.refresh_in only


def matrix(code, env):
    """[(principal, scope, privileges)] for the catalog <code>_<env> (01_grant_access applies exactly this)."""
    sp = f"sp_{code}_{env}"
    m = {
        # 22 Oct: the three dev roles also get MANAGE, so each person's own dev job may replace the mask functions,
        # views and Gold tables another person created first (shared synthetic sandbox; test and prod unchanged).
        "dev": [
            ("lh_developer", "catalog", RW + MANAGE),
            ("lh_teamlead", "catalog", RW + MANAGE),
            ("lh_manager", "catalog", RW + MANAGE),
            ("lh_management", "catalog", R + MANAGE),
        ],
        "test": [
            ("lh_developer", "catalog", R_DATA),
            ("lh_teamlead", "catalog", R_DATA),
            ("lh_manager", "catalog", R_DATA + MANAGE),
            ("lh_bizmanager", "gold", GOLD_R),
            ("lh_management", "catalog", R + MANAGE),
            (sp, "catalog", RW),
        ],
        "prod": [
            ("lh_developer", "catalog", R_DATA),
            ("lh_teamlead", "catalog", R_DATA),
            ("lh_manager", "catalog", R_DATA),
            ("lh_bizmanager", "gold", GOLD_R),
            ("lh_management", "catalog", R + MANAGE),
            (sp, "catalog", RW),
        ],
    }[env]
    if env in ("test", "prod"):
        m += [("lh_ci", "gold", CI_GOLD), ("lh_ci", "ops", CI_OPS)]
        if code == "fqf":
            m.append((f"sp_bhg_{env}", "gold", GOLD_R))  # the BHG job builds the group view from fqf_<env>.gold
    return m


def refresh_grants(code, env):
    """Grants made by setup_once/notebook/03_refresh_setup on <code>_<env> (as target): [(principal, object, privs)]
    with object = ("CATALOG" | "SCHEMA" | "VOLUME", name)."""
    cat = f"{code}_{env}"
    out = []
    for src, tgt in REFRESH:
        if tgt == env:
            sp = f"sp_{code}_{src}"
            out += [
                (sp, ("CATALOG", cat), ["USE CATALOG"]),
                (sp, ("SCHEMA", f"{cat}.ops"), ["USE SCHEMA"]),
                (sp, ("VOLUME", f"{cat}.ops.refresh_in"), REFRESH_VOLUME),
            ]
    return out


def expected(code, env):
    """{(object type, object name): {(principal, privilege)}} for every object that carries direct grants."""
    cat = f"{code}_{env}"
    out = {("CATALOG", cat): set()}
    for name, scope, privs in matrix(code, env):
        if scope == "catalog":
            out[("CATALOG", cat)] |= {(name, p) for p in privs}
        else:
            out[("CATALOG", cat)] |= {(name, "USE CATALOG"), (name, "BROWSE")}
            out.setdefault(("SCHEMA", f"{cat}.{scope}"), set()).update((name, p) for p in privs)
    for name, obj, privs in refresh_grants(code, env):
        out.setdefault(obj, set()).update((name, p) for p in privs)
    return out
