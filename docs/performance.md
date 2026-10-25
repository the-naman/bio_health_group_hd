# Performance and cost

Same file in both repos. How fast and how big the platform is, what was changed for speed and cost, and what was
left alone on purpose. Measured with a read-only baseline (table sizes from DESCRIBE DETAIL, task minutes from the
Jobs API, last 7 days of test and prod). Free Edition hides billing tables, so cost is measured in task minutes.

## Baseline (24 Oct 2026, before the changes)
| Catalog | Bronze | Silver | Gold | ops | Files |
|---|---|---|---|---|---|
| bhg_prod | 2.1 MB | 1.9 MB | 2.7 MB | 0.1 MB | 128 |
| bhg_test | 1.3 MB | 2.4 MB | 3.4 MB | 0.1 MB | 202 |
| fqf_prod | 1.3 MB | 1.1 MB | 0.8 MB | 0.2 MB | 227 |
| fqf_test | 1.0 MB | 1.3 MB | 0.8 MB | 0.1 MB | 265 |
Largest table: `bhg_test.gold.fact_bhg_event` 2.4 MB. Small-file tables: `silver.payment_events` 58-59 files of
4 KB, `gold.payment_failure_windows` 26 files, `ops.audit_runs` / `ops.dq_results` 24-31 files of 2 KB each.

| Job (median minutes per run) | Bronze | Silver | Group | Gold | Rest | Run |
|---|---|---|---|---|---|---|
| lh_bhg_prod_daily | 7.1 | 3.6 | 1.7 | 1.1 | 2.2 | about 16 |
| lh_fqf_prod_daily | 9.3 | 5.1 | - | 1.1 | 2.2 | about 17 |
| lh_bhg_test_daily | 5.2 | 2.9 | 1.8 | 1.0 | 1.9 | about 13 |
| lh_fqf_test_daily | 3.4 | 2.0 | - | 1.0 | 1.9 | about 8 |
| lh_fqf_prod_stream | bronze 1.1, silver 0.7, gold 0.6 | | | | | about 2.4 |
Task minutes in 7 days: bhg_test 99, fqf_test 67, bhg_prod 53, fqf_prod 52, streams 16 (prod since 23 Oct).

## Findings
1. Bronze is half of every daily run, although the data is tiny. It loaded its 11-14 tables one after the other, and
   each table pays a fixed start-up (Auto Loader availableNow run, two counts, one audit row) of about 30 seconds.
2. Data volume is not the problem: the whole platform is about 30 MB. Partitioning, liquid clustering or Z-order would
   add work and give nothing at this size.
3. Append-only tables (stream steps every 2 hours, run logs) collect many tiny files, which slows the dashboard page
   "Payment events" and every alert query a little more each day.
4. Found in the first dev run after the changes: the data dictionary set every Gold column description again on
   each run (BHG 113, FQF 82 ALTER statements, about 1.5-2 minutes of the BI step), because the full Gold overwrite
   replaces the schema and drops column comments.
6. Found in test after the promotion (24 Oct): Bronze took about 2 min 45 s even with 0 new files; the loading itself
   took 0 s. Measured 24 Oct (read-only probe, 0 files): one recursive Spark read of landing = dev 35 s, prod 153-161 s;
   dbutils.fs.ls with 8 threads = dev 20 s, prod 78-82 s; Files API = dev 25 s, prod 105 s (prod has many more
   source/table/year/month folders). Fixed: the names are listed level by level with 32 listings at once and the
   contents are read only when there are files (no Spark job on a 0-file day); every run prints "landing listed in".
7. `silver_rebuild` shows "not success" on every run: that is the task being skipped on purpose (it only runs after a
   failed Silver checkpoint), not a failure.

## Changes
| Change | Where | Expected effect |
|---|---|---|
| Bronze loads 4 tables side by side | `bronze.load_all`, `config/performance.yml` bronze_parallel_tables | Bronze 7-9 -> about 3 min; daily runs about 25-35% shorter; about 20 task minutes less per day over the four runs |
| Landing listed level by level in parallel; contents read only when there are files | `bronze.list_landing`, `config/performance.yml` landing_list_threads | Bronze with 0 files: prod about 2.7 -> well under 1 min (to confirm in prod) |
| Gold column descriptions written with the data | `gold.save` (`with_comments`), `src/lh/dictionary.py` | BI step about 2.5 -> about 1 min; a normal day sets 0 descriptions |
| Daily compaction of small-file tables | `src/lh/maintain.py`, BI step, `config/performance.yml` compact | OPTIMIZE only when a table has 50+ files averaging under 8 MB; a normal day costs one DESCRIBE per table |
Each table still has its own checkpoint, files and audit row; a failing table no longer stops the others, the task
still fails and retries. Setting bronze_parallel_tables to 1 restores the old behaviour.

## Left alone on purpose
| Option | Why not now | When it would pay |
|---|---|---|
| Liquid clustering on the facts (date_key) | 2.4 MB fact, a full scan takes under a second | facts above about 1 GB, or dashboards filtering by date |
| Parallel Silver | tables depend on each other (parents, id map, reconciliation) | many more independent tables |
| Fewer, larger tasks | separate tasks give separate retries and alerts | never: reliability first |
| Photon / bigger compute | serverless chooses compute on Free Edition | paid workspace with job clusters |

## After
Dev, 24 Oct (no new source files that day, so Bronze had nothing to load: 3 s FQF, 0 s BHG; the Bronze gain shows on
days with files, measured in test and prod after promotion):
| Check | FQF dev | BHG dev |
|---|---|---|
| Run | 8 min 17 s, all green | 8 min 3 s, all green |
| Compaction | silver.payment_events 88 -> 1 file | nothing needed |
| BI step | 2 min 44 s (dictionary 82 columns set again: fixed after this run) | 2 min 38 s (113 columns: fixed) |

| Job (test/prod, after promotion) | Bronze before | Bronze after | Run before | Run after |
|---|---|---|---|---|
| lh_fqf_prod_daily | 9.3 | 4.1 (13 files, 185 rows: listing 81 s, load 139 s) | about 17 | 13.3 (25 Oct, scheduled) |
| lh_bhg_prod_daily | 7.1 | 3.7 (11 files, 218 rows: listing 85 s, load 117 s) | about 16 | 14.4 (25 Oct, scheduled) |

Result: on a normal day with one new file per table, Bronze is 50-55% shorter and whole prod runs about 15-20% shorter.
On a day with no new files (24 Oct evening, after the parallel listing): FQF prod 7.2 min, BHG prod 9.7 min, landing
listed in 42 s and 84 s. The rest of each run (Silver, masks, Gold, group, BI) is unchanged by design.

7. Found 25 Oct: the GitHub scheduler ran the payment-stream producer (`stream.yml`, cron minute :30) only 3 of 8 times
   on 24 Oct and the simulator (:31) about 2.5 h late, while workflows at :15 ran close to time. The producer moved to
   minute :07 (06:37-20:37 IST, about 50 minutes before each stream run). The stream health alert reports any gap.
