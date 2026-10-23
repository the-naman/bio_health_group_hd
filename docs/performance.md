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
4. `silver_rebuild` shows "not success" on every run: that is the task being skipped on purpose (it only runs after a
   failed Silver checkpoint), not a failure.

## Changes
| Change | Where | Expected effect |
|---|---|---|
| Bronze loads 4 tables side by side | `bronze.load_all`, `config/performance.yml` bronze_parallel_tables | Bronze 7-9 -> about 3 min; daily runs about 25-35% shorter; about 20 task minutes less per day over the four runs |
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

## After (fill in after the dev and test runs)
| Job | Bronze before | Bronze after | Run before | Run after |
|---|---|---|---|---|
| dev FQF | | | | |
| dev BHG | | | | |
