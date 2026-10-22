# bio_health_group_hd
Lakehouse pipelines of Bio Health Group (BHG), the parent company, and the group view that consolidates
its child company Flexi Quant Finance (FQF): BHG Gold + FQF Gold of the same environment, read-only.

## Layout
| Folder | What it holds |
|---|---|
| `simulator/` | synthetic source data generator (a GitHub Action writes the files to the S3 landing folder) |
| `bronze/`, `silver/` | load raw files with Auto Loader, then clean, standardise and keep history (`src/lh/bronze.py`, `silver.py`) |
| `gold/` | Gold runners. The Gold logic itself is SQL, one SELECT per table: `gold/sql/<table>.sql` (company) and `gold/sql/group/<table>.sql` (group view of both companies) |
| `security/` | column masks and row filters, driven by `config/privacy.yml` |
| `bi/` | BI views and AI/BI dashboards (published per environment) |
| `jobs/` | the daily job as code (`config/jobs.yml`) |
| `checks/` | automated checks (t00 = smoke check of every daily run, t28 = deployed jobs match config, t29 = each person's own access matches the access matrix) |
| `ci/`, `.github/workflows/` | promotion checks, approval notices, deploy after a merge, business notices, alerts and the weekly digest |
| `config/` | every setting, per environment. No secrets: they live in GitHub secrets and Databricks secret scopes |
| `setup_once/` | one-time environment setup and access grants (admin) |

## One framework, every table on its own (requirement 17)
Bronze and Silver are metadata-driven: one engine per layer (`src/lh/bronze.py`, `src/lh/silver.py`) and one
config entry per table (`config/tables.yml`, `config/silver.yml`). Gold is one SQL file per table. Every table
still has its own load: its own Auto Loader checkpoint and files, Silver watermark, quarantine rows, audit row
(`bronze.<table>`) and checkpoint result. To work on a single table, open `bronze/01_load_bronze` or
`silver/01_load_silver` in your dev Git folder, set `env = dev` and `table = <name>` and run it; the default
`table = all` is what the daily job runs. A new table = one config entry (+ its check), no new notebook.

## How code moves
- Work happens on `bhg_dev`: each person runs their own dev job (`lh_bhg_dev_daily_<role>`).
- `bhg_dev` -> `bhg_test` and `bhg_test` -> `bhg_prod` only by pull request. Checks run first; the Manager
  approves test, the Business-user Manager approves prod. A rejection sends the change back to the lower branch.
- After a merge, CI pulls the environment's Git folder, redeploys the daily job and checks it (`checks/t28_jobs`).
- Only code moves between environments, never data. No personal data, address or secret is ever committed.

## Emergency hotfix (10j)
For a prod problem that cannot wait for the normal path (rules in `config/ci.yml`, checked by `ci/route.py`):
1. Branch `bhg_hotfix_<name>` from `bhg_prod`, make the fix, push.
2. Pull request into `bhg_test`: same checks, URGENT approval by Management (environment `bhg_test_hotfix`).
   Merge with "Create a merge commit".
3. Pull request into `bhg_prod`: accepted only once the fix is in test and the branch holds only prod + the fix;
   the Business-user Manager approves.
4. After the prod deploy, `backmerge.yml` (`ci/backmerge.py`) opens a pull request `bhg_prod -> bhg_dev`
   so dev keeps the fix. Merge it with "Create a merge commit", then delete the hotfix branch.

## Deciding look-alike pairs (Manager)
`gold.group_match_review` lists BHG patient / FQF customer pairs the matching does not merge on its own: the same
phone and date of birth with a different Aadhaar, or a hospital bill paid by a loan of a different Aadhaar.
In test and prod the two ids are shown as tokens, so a decision is recorded by `review_key` (not personal data,
never masked). Check the pair with the source teams (hospital front desk, loan officer) first; never copy a
name, phone, Aadhaar or date of birth anywhere.
1. Open pairs (SQL editor, catalog `bhg_<env>`):
   `SELECT review_key, reason FROM gold.group_match_review WHERE status = 'open'`
2. Record the decision (`same` = one person, `different` = two people); `decided_by` is the role, never a name:
   `INSERT INTO ops.group_match_decisions (review_key, decision, decided_by, decided_at)
    VALUES (<review_key>, 'same', 'manager', current_timestamp())`
3. The next daily run applies it (`gold/sql/group/_grp_decisions.sql`): a `same` pair becomes one person
   (`match_method = review`) and leaves the review list; a `different` pair stays listed as `decided`.
   To change a decision, insert a new row for the same `review_key`: the latest one wins.

## Controlled refresh (5.11)
Test and dev can get a safe copy of real-looking data: prod -> test (half of the people), test -> dev (a tenth).
Rules in `config/refresh.yml`, logic in `src/lh/refresh.py`. People are picked by a keyed hash of their national id
(the same people in BHG and FQF) with all their rows; reference rows come complete. Personal values are faked, the ids
of `config/privacy.yml` are re-keyed with a secret key, amounts are kept. The source job account writes only sanitised
files into the target's `ops.refresh_in`; the target adds them to its Silver (its own rows stay) and rebuilds Gold.
1. Actions -> **refresh** -> Run workflow, "Use workflow from" = the prod branch, target = test. Approve the
   `<code>_refresh` environment. Do FQF first, then BHG.
2. The workflow runs `refresh/01_refresh_out` (source), `refresh/02_refresh_in` and the normal test job (target).
3. Check: `checks/t31_refresh` with env = test.
For dev: same with target = dev; then each person runs `refresh/02_refresh_in` (env dev) and their own dev job.
A Silver rebuild from Bronze (`silver_rebuild`) removes the refreshed rows; run the refresh again after one.

## Alerts and weekly digest (5.12)
Four kinds of mail, each to the people who act on it:

| Mail | Sent by | When |
|---|---|---|
| a task failed | Databricks (`config/jobs.yml` notify) | at once, to the fixers |
| figures delayed / up to date again, FQF merged | `ci/notices.py` (`notices.yml`) | business notice, once each |
| data alert | `ci/alerts.py` (`alerts.yml`, every 2 h) | refused file, Silver rebuilt from Bronze, quarantine spike, new source column, BHG: group merge waiting, over-limit customers changed |
| weekly digest | `ci/alerts.py --digest` (Monday 09:15 IST) | runs and failures, freshness, quarantine, new columns, group numbers, mails sent |

Rules and recipients: `config/alerts.yml`. Each finding is mailed once (`ops.notice_log`, `alert_<kind>`); the
scheduled run is quiet until `live` is switched on at go-live. Alerts read only the health views
(`gold.v_health_*`) and the ops logs as `sp_lh_notices`; no personal values, no row keys. The new-column watch
(`src/lh/observe.py`, run by `bi/01_create_views`) keeps only the column NAMES found in Bronze `_rescued_data`
(`ops.rescued_keys`); a new name is planned by adding it to `from_rescued` in `config/silver.yml` through a PR.
Try it: Actions -> **alerts** -> Run workflow, env test, what alerts or digest, mode dry-run or sample.
