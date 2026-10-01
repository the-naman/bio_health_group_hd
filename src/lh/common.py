import os, hmac, hashlib, re, datetime, yaml

def repo_root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_config(env):
    root = repo_root()
    with open(f"{root}/config/company.yml") as f:
        cfg = yaml.safe_load(f)
    with open(f"{root}/config/{env}.yml") as f:
        cfg.update(yaml.safe_load(f))
    return cfg

def match_key(env):
    from databricks.sdk.runtime import dbutils
    return dbutils.secrets.get(f"lh_{env}", "group_match_key")

def fingerprint(value, key):
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 12:
        return None
    return hmac.new(key.encode(), digits.encode(), hashlib.sha256).hexdigest()

def log_run(spark, cfg, job, status, rows=0, message=""):
    t = f"{cfg['catalog']}.ops.audit_runs"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {t} (
        run_ts TIMESTAMP, company STRING, env STRING, job STRING,
        status STRING, rows BIGINT, message STRING)""")
    spark.createDataFrame(
        [(datetime.datetime.utcnow(), cfg["code"], cfg["env"], job, status, int(rows), message[:500])],
        f"run_ts timestamp, company string, env string, job string, status string, rows bigint, message string"
    ).write.mode("append").saveAsTable(t)