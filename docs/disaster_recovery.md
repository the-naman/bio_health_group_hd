# Disaster recovery

What can be lost, where it comes back from, and how long that takes. Same file in both repos; covers BHG and FQF.
Day-to-day failures (a failed task, a refused file) are in `docs/runbook.md`. No names, addresses or secrets here.

## 1. Principles
- Code is the source of truth for everything except data: pipelines, jobs, grants, masks, dashboards, alerts and the
  data dictionary are rebuilt from Git and `config/`; nothing is configured only by hand without a notebook or workflow.
- Data is rebuilt forwards, not restored backwards. Tables with column masks or row filters refuse Delta time travel
  and RESTORE, so each layer is rebuilt from the layer below: Gold from Silver (every run), Silver from Bronze
  (`silver/02_rebuild_silver`), Bronze from the source files kept in `bronze/processed/`.
- Secrets have one home outside Databricks: GitHub secrets (or the Aiven console for Kafka). Scopes are refilled from there.
- Everything is per environment: dev, test and prod are separate catalogs, buckets, scopes and job accounts, so a loss
  in one never needs another to be touched.

## 2. Assets, targets and recovery
RPO = data that may be lost; RTO = time to be back. Times are measured on this platform (prod daily run about 25 minutes).

| Asset | Lives in | Comes back from | How | RPO | RTO |
|---|---|---|---|---|---|
| Code, config, notebooks | GitHub, 3 branches per repo (+ every Databricks Git folder) | GitHub; any Git folder if GitHub is lost | re-clone; or push a Git folder to a new repo | 0 | minutes |
| Jobs (daily, stream) | Databricks Jobs | `config/jobs.yml`, `config/stream.yml` | deploy workflow (any merge) or `jobs/01_deploy_jobs` apply | 0 | 10 min |
| Dashboards | Databricks AI/BI | `bi/*.lvdash.json` | task `publish` (any daily run) | 0 | 15 min |
| Gold tables, BI views, comments | `<code>_<env>.gold` | Silver | rerun the daily job (Gold is rebuilt in full; masks, row filter and dictionary are re-applied) | 0 | 30 min |
| Silver tables | `<code>_<env>.silver` | Bronze | automatic when the Silver checkpoint fails; by hand: `silver/02_rebuild_silver` | 0 | 30-60 min |
| Bronze tables | `<code>_<env>.bronze` | source files in `bronze/processed/` (S3 bucket of the env) | copy the files back into `landing/`, run the daily job, then rebuild Silver (section 4, not drilled yet) | 0 | 1-2 h |
| Source files | S3 `lh-<code>-<env>-source` (landing, processed, rejected) | the simulator (same input, same files) | Actions -> simulator for the missing days | 0 (synthetic) | 30 min |
| Stream tables | `fqf_<env>` bronze/silver/gold `payment_events`, `payment_failure_windows` | Kafka topic (keeps 7 days) | drop the table and its checkpoint folder `ops/checkpoints/stream/<layer>`, next run reads from the earliest offset | events older than 7 days | 1 stream run |
| Run and notice logs | `ops.audit_runs`, `ops.notice_log`, `ops.dq_results` | not backed up (logs) | recreated on the next run; a notice or alert may be sent once more | history | next run |
| Catalogs, schemas, volumes, grants | Unity Catalog | `setup_once/notebook/00_setup_environment`, `01_grant_access`, `03_refresh_setup` | run them (Management / admin), then `02_verify_setup` | 0 | 1 h |
| Masks, row filter | Unity Catalog | `config/privacy.yml` | applied by task `masks` and the Gold runner on every run | 0 | next run |
| Secret scopes | Databricks `lh_dev`, `lh_test`, `lh_prod`, `lh_refresh` | GitHub secrets | Actions -> setup-secret-scopes (approval in `bhg_admin`); `lh_refresh`: `03_refresh_setup` + the refresh workflow | 0 | 30 min |
| Service principals | Databricks account | this document + `02_verify_setup` (expected names, groups, entitlements) | recreate, new OAuth secret into GitHub secrets, `01_grant_access` apply | 0 | 1-2 h |

## 3. Scenarios
**A. A bad change reached prod.** Do not edit prod. Revert or fix on a hotfix branch from `<code>_prod` (README
"Emergency hotfix"), test -> prod by pull request; the back-merge brings it to dev. Data written by the bad code is
fixed by the next daily run (Gold rebuilt) or by a Silver rebuild if Silver was affected.

**B. Silver is wrong or a Silver table was dropped.** Run `silver/02_rebuild_silver` (dev: `force = yes`; test/prod:
it runs on its own after a failed checkpoint, or as a Repair run of the daily job). It empties Silver (tables, masks
and filters stay), resets the watermarks and replays Bronze. A dropped table is recreated by the replay. Then rerun
Gold. After a rebuild in test or dev, run the controlled refresh again if refreshed rows were there.

**C. A Gold table or BI view was dropped.** Repair run of the daily job from task `gold`: tables, masks, row filter,
views, data dictionary and dashboards come back.

**D. A Bronze table was dropped (not drilled yet).** As Management in a scratch notebook of that env: copy the table's
files from `/Volumes/<code>_<env>/bronze/processed/<source>/<table>/` back to `.../landing/<source>/<table>/`, run the
daily job (Bronze reloads and files them away again), then rebuild Silver (B). Bronze keeps every row as sent, so the
result equals the lost table; Silver de-duplicates by key.

**E. Stream checkpoint or stream table lost (FQF).** Stop nothing (runs are short). Remove the broken table and its
checkpoint folder for that layer and every layer above it; the next run reads Kafka from the earliest kept offset
(7 days) and rebuilds downstream. Velocity alerts already sent are not sent again (`ops.notice_log`).

**F. A secret leaked or a person left.** Rotate the secret where it lives (GitHub secret, Aiven user password,
Databricks OAuth secret or token), then refill the scopes (setup-secret-scopes with rotate only for the match key);
remove the person from their group (access follows the group). The group match key is special: Bronze stores
fingerprints made with it, so a new key needs a Bronze replay (D) of that environment; the prod key is kept as the
GitHub secret `GROUP_MATCH_KEY` and is never rotated casually.

**G. Workspace lost.** In order: (1) new workspace, Unity Catalog metastore, storage credentials and external
locations to the same S3 buckets (`00_setup_environment`); (2) groups and service principals (section 2), OAuth secrets
into GitHub; (3) `01_grant_access` apply for each env, `03_refresh_setup`; (4) setup-secret-scopes; (5) role Git
folders and credentials; (6) promote or deploy to create the jobs; (7) daily jobs in order FQF, then BHG: Bronze
reloads from `processed/` copied back to `landing/` (D), Silver and Gold follow; (8) stream jobs re-read Kafka;
(9) `02_verify_setup`, `t28_jobs`, `t29_role_access`, `t00` smoke. Expected effort about one working day; data loss
none for batch data, stream events older than 7 days.

**H. GitHub repository lost.** Every Databricks Git folder (each role, the deploy account, the owner's break-glass
folders) holds the full history: create the repo again, push all three branches from a folder that is up to date,
recreate secrets, environments, rulesets (README) and upload the workflows.

## 4. Known gaps (accepted on Free Edition, would change in a company)
| Gap | Risk | In a company |
|---|---|---|
| S3 versioning off on the source buckets | a deleted source file is gone (here: the simulator can recreate it) | versioning + lifecycle on prod buckets, cross-region replication |
| One region, one workspace | region outage stops everything | second workspace in another region, Terraform or bundles for setup |
| Bronze replay (D) and workspace rebuild (G) not drilled | steps may need fixing when needed | drill twice a year, record the timing |
| Logs (`ops.*`) not backed up | lose run history | export logs or use system tables |
| Kafka keeps 7 days | a stream outage longer than 7 days loses events | longer retention or a sink of raw events to S3 |

## 5. Drills
| What | Result | When |
|---|---|---|
| Hotfix path (A), test part | PR into test with URGENT approval, deploy and `t28` green | 16 Oct 2026 |
| Silver rebuild from Bronze (B) | automatic path coded and wired in the daily job; drill by hand in dev with `force = yes` | to repeat yearly |
| Controlled refresh prod -> test -> dev | `t31_refresh` PASS in test and dev | 22-23 Oct 2026 |
| Stream continues from its checkpoint | second prod and test runs read only new events, no event twice | 23 Oct 2026 |
| Bronze replay (D), workspace rebuild (G) | not drilled | open |
