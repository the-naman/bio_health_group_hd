"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only; mail addresses are
built at run time (AT) so no address-shaped text is ever committed."""

AT = chr(64)
import sys
import json
import datetime as dt
import os

sys.path.insert(0, os.path.join(sys.argv[1], "src"))
from pyspark.sql import SparkSession, types as T

spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
import yaml
from lh import stream

scfg = yaml.safe_load(open(os.path.join(sys.argv[1], "config", "stream.yml")))
W = os.path.join(sys.argv[2], "work")
KSCH = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)
off = [0]


def send(events, batch):
    rows = []
    for e in events:
        v = e if isinstance(e, str) else json.dumps(e)
        rows.append((b"k", v.encode(), "fqf_payment_events_dev", 0, off[0], dt.datetime(2026, 10, 23, 10, 0)))
        off[0] += 1
    spark.createDataFrame(rows, KSCH).write.mode("append").parquet(f"{W}/kafka/b{batch}")


def run(layer):
    if layer == "bronze":
        src = stream.to_bronze(spark.readStream.schema(KSCH).parquet(f"{W}/kafka/*"))
    elif layer == "silver":
        src = stream.to_silver(
            spark.readStream.schema(spark.read.parquet(f"{W}/bronze").schema).parquet(f"{W}/bronze"), scfg
        )
    else:
        src = stream.to_gold(
            spark.readStream.schema(spark.read.parquet(f"{W}/silver").schema).parquet(f"{W}/silver"), scfg
        )
    q = (
        src.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", f"{W}/cp/{layer}")
        .trigger(availableNow=True)
        .start(f"{W}/{layer}")
    )
    q.awaitTermination()


def all3():
    for l in ("bronze", "silver", "gold"):
        run(l)


T0 = dt.datetime(2026, 10, 23, 10, 0)


def ev(i, mins, cust="SC001", status="success", amt=10000):
    return {
        "event_id": f"E{i}",
        "event_ts": (T0 + dt.timedelta(minutes=mins)).isoformat(),
        "customer_ref": cust,
        "loan_ref": "SL1",
        "amount_paise": amt,
        "status": status,
        "method": "upi",
        "reason": None if status == "success" else "insufficient_funds",
    }


ok = []


def rd(p):
    return spark.read.parquet(p)


def c(n, x, d=""):
    ok.append(bool(x))
    print(("PASS " if x else "FAIL ") + n + ("" if x else f" -> {d}"))


# batch 1: burst of 4 failures for SC009 at 10:01-10:04, normal events, 2 duplicates, 1 bad JSON
b1 = (
    [ev(i, i % 9) for i in range(1, 11)]
    + [ev(100 + k, 1 + k, "SC009", "failed") for k in range(4)]
    + [ev(3, 3), ev(100, 1, "SC009", "failed")]
    + ["{not json"]
    + [ev(200, 2, "SC002", "failed")]
)
send(b1, 1)
all3()
br = rd(f"{W}/bronze")
sv = rd(f"{W}/silver")
c("bronze keeps every message (17)", br.count() == len(b1), br.count())
c("silver: bad JSON left out, duplicates removed (15 unique)", sv.count() == 15, sv.count())
c("silver: event_id unique", sv.select("event_id").distinct().count() == sv.count())
c("silver: amount in rupees", sv.filter("event_id='E1'").first()["amount"] == 100)
g = rd(f"{W}/gold") if os.path.exists(f"{W}/gold/_spark_metadata") else None
c("gold: nothing yet (window not closed by watermark)", g is None or g.count() == 0, g and g.count())
# rerun with nothing new: nothing read twice
all3()
c("rerun: bronze unchanged", rd(f"{W}/bronze").count() == len(b1))
c("rerun: silver unchanged", rd(f"{W}/silver").count() == 15)
# batch 2 (an hour later): new events at 11:00 move the watermark past 10:10 + 15 min; a 2-hours-late event; a dup of E5
b2 = [ev(300 + i, 60 + i) for i in range(5)] + [
    ev(400, -120, "SC777", "failed"),
    ev(5, 5),
    ev(401, 58, "SC003", "failed"),
]
send(b2, 2)
all3()
sv = rd(f"{W}/silver")
c("silver: 2-hours-late event dropped", sv.filter("event_id='E400'").count() == 0)
c(
    "silver: duplicate of E5 from the next batch dropped (within watermark state)",
    sv.filter("event_id='E5'").count() == 1,
)
c("silver: new events in (15 + 6)", sv.count() == 21, sv.count())
# batch 3: move watermark past 11:10 so later windows close too
send([ev(500, 140)], 3)
all3()
send([ev(501, 200)], 4)
all3()  # a closed window is written by the next run that receives data (state lags one run)
g = rd(f"{W}/gold")
rows = {(r.customer_ref, str(r.window_start)): r for r in g.collect()}
c(
    "gold: SC009 window 10:00-10:10 has 4 failed, is_alert",
    rows.get(("SC009", "2026-10-23 10:00:00")) is not None
    and rows[("SC009", "2026-10-23 10:00:00")].failed_payments == 4
    and rows[("SC009", "2026-10-23 10:00:00")].is_alert,
    list(rows),
)
c(
    "gold: duplicate failed event not counted twice (4 not 5)",
    rows[("SC009", "2026-10-23 10:00:00")].failed_payments == 4,
)
c(
    "gold: SC002 1 failed, no alert",
    rows.get(("SC002", "2026-10-23 10:00:00")) and not rows[("SC002", "2026-10-23 10:00:00")].is_alert,
)
c("gold: late SC777 never counted", not any(k[0] == "SC777" for k in rows))
c("gold: SC003 (10:58) window closed and written", any(k[0] == "SC003" for k in rows), list(rows))
c("gold: failed amount in rupees (4 x 100)", rows[("SC009", "2026-10-23 10:00:00")].failed_amount == 400)
c("gold: one row per window and customer", g.count() == g.select("window_start", "customer_ref").distinct().count())
# options builder: no secret in the topic, right user per env
o = stream.kafka_options(
    scfg, "test", lambda k: {"kafka_password": "PW", "kafka_bootstrap": "h:1", "kafka_ca_cert": "CA"}[k]
)
c(
    "kafka options: topic/user per env, SASL_SSL SCRAM, PEM truststore",
    o["subscribe"] == "fqf_payment_events_test"
    and 'username="lh_consumer_test"' in o["kafka.sasl.jaas.config"]
    and o["kafka.security.protocol"] == "SASL_SSL"
    and o["kafka.sasl.mechanism"] == "SCRAM-SHA-256"
    and o["kafka.ssl.truststore.type"] == "PEM",
)
print(f"\nstream tests: {sum(ok)} of {len(ok)} ->", "PASS" if all(ok) else "FAIL")
