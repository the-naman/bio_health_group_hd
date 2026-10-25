# Operations runbook

How the lakehouse of Bio Health Group (BHG, parent) and Flexi Quant Finance (FQF, acquired) is run day to day, and
what to do when something goes wrong. Same file in both repos. Times are IST. No names, addresses or secrets here:
people are named by role, mail addresses live only in GitHub secrets and secret scopes. Disaster recovery (losing a
table, a checkpoint, a secret or the workspace) is in `docs/disaster_recovery.md`.

## 1. Roles
| Role | Group | Does |
|---|---|---|
| Developer, Team Lead | `lh_developer`, `lh_teamlead` | build in dev (own dev job), fix failures, open pull requests |
| Manager | `lh_manager` | runs and repairs test jobs (MANAGE in test), decides look-alike pairs |
| Management | `lh_management` | platform admin: prod repairs, grants, job owners, secret scopes; never writes data |
| Business Manager | `lh_bizmanager` | reads prod dashboards (masked, own hospitals only) |
| Job accounts | `sp_<code>_<env>` | run the test/prod jobs; the only writers in test and prod |
| CI accounts | `sp_lh_deploy`, `sp_lh_notices` | deploy jobs after a merge; read health views for notices and alerts |

## 2. The day (IST)
| Time | What | Check |
|---|---|---|
| 00:01, 01:01 | simulator writes yesterday's source files (safety run writes nothing if done); GitHub may start it hours late, then its chain job reruns the daily job | Actions -> simulator green (simulate + chain) |
| 03:00 / 04:00 | prod daily job FQF, then BHG (group view reads FQF Gold) | smoke `t00` at the end of the run |
| 05:00 / 06:00 | test daily job FQF, then BHG | smoke `t00` |
| 03:45-19:45 / 2 h | business notices (figures delayed, up to date again, FQF merged) | sent once each |
| 04:45-20:45 / 2 h | prod data alerts; 07:05 test alerts; Monday 09:15 digest; 20:45 dev activity | sent once each |
| 06:37-20:37 / 2 h | producer sends payment events to Kafka (test, prod) | Actions -> stream green |
| 07:30-21:30 / 2 h | `lh_fqf_prod_stream`; test 08:50, 14:50, 20:50 | stream health alert |

Morning check (5 minutes, Developer): mailbox (failures, alerts, notices); Jobs -> the four test/prod daily runs green;
dashboard `data_health_<env>`: freshness 0-1 day behind, no open quarantine growth; Actions: no red scheduled run.

## 3. When something goes wrong
Start with the mail: every automatic mail names what failed. Fix in dev, move the fix by pull request; never edit
test or prod by hand. A prod problem that cannot wait uses the hotfix path (README "Emergency hotfix").

| Signal | First look | Usual fix | Who |
|---|---|---|---|
| Databricks mail "task failed" (test/prod) | Jobs -> the run -> failed task output | transient (timeout, cluster, network): Repair run. Code or data: fix in dev, promote (or hotfix) | fixers; repair: Manager (test), Management (prod) |
| Silver checkpoint failed | task `silver_rebuild` ran on its own (Silver rebuilt from Bronze) | if the rebuild passed: nothing; if it failed: read its output, fix in dev | fixers |
| Notice "figures are delayed" | the last prod run (failed, stuck over 3 h, or none for 26 h) | repair or start the run; the "up to date again" notice follows by itself | Management |
| Alert "files refused by Bronze" | file name and reason in the mail; file in `bronze/rejected/` | ask the source for a corrected file and put it into `landing/`; the next run loads it | Developer |
| Alert "quarantine spike" | dashboard data_health, view `v_health_quarantine` (table, rule, count) | source data problem: tell the source owner; rule problem: fix the rule in dev | Developer, Team Lead |
| Alert "new source columns" | `v_health_rescued` (column names only) | to use it: add to `from_rescued` in `config/silver.yml` by pull request; else ignore | Team Lead |
| Alert "group merge waiting" (BHG) | FQF daily run of that env | run FQF, then BHG task `group` (Repair run) | Management (prod) |
| Alert "over-limit customers" (BHG) | dashboard group_overview, `v_group_exposure` | business information, no technical action | Manager |
| Alert "failed-payment velocity" (FQF) | `v_fqf_payment_alerts` (customer reference is a token) | business follow-up of that customer | Manager |
| Alert "payment stream health" (FQF) | Jobs -> `lh_fqf_<env>_stream`; Actions -> stream | no finished run: repair the job, check the Kafka login (scope `lh_<env>`); no events: check the producer run and the topic; many left out: look at broken/late rows in Bronze | Developer |
| GitHub mail "Deploy FAILED" | Actions -> deploy -> the log | new job: "only workspace admins can change the owner": Management sets the job account Is Owner and `sp_lh_deploy` Can Manage, then Re-run all jobs. Else fix and re-run | Management, fixers |
| Smoke WARN "this run loaded new source files" (0 files) | Actions -> simulator: did it run before the job? | GitHub started the simulator late; its chain job (`ci/chain.py`) starts the daily job again (FQF, then BHG) once the files are written. If the simulator did not run at all: run it by hand (env test/prod); the chain follows | Developer |
| A GitHub check hangs or a scheduled run is missing | Actions -> the run | Re-run all jobs; run the workflow by hand (simulator, alerts, stream) | Developer |
| Dashboard empty or old | task `publish` of the last run; `checks/t27_bi_publish` | repair the run from `bi_views` | Developer |
| Someone cannot see or do something | `checks/t29_role_access` as that person | `setup_once/notebook/01_grant_access` preview, then apply (Management); verify with `02_verify_setup` | Management |

Repairs are safe to repeat: every load is idempotent (Bronze files move to `processed/` once loaded, Silver merges by
key, Gold is rebuilt in full, stream steps resume from their checkpoints, every alert or notice is sent once).

## 4. Changes
- Normal: `<code>_dev` -> pull request -> `<code>_test` -> pull request -> `<code>_prod`. Checks (lint, tests, shared-file
  guard, job config) run on every pull request; an environment reviewer approves; the deploy and `t28_jobs` follow by
  themselves. Change shared code in both repos before promoting (the drift guard fails otherwise).
- New Gold column or BI view: describe it in `config/dictionary.yml` in the same pull request.
- Hotfix: README "Emergency hotfix"; the back-merge pull request brings the fix to dev.
- Workflow files (`.github/workflows/`) are uploaded by the repo owner (role credentials have no workflow scope).
- While GitHub keeps the role accounts flagged (support tickets open), their pull requests are hidden and start no
  workflow: the-naman opens every pull request and names who prepared it (template). Role accounts still push to dev.
- A shared file that only the owner can upload (a workflow): upload it to both repos before opening the pull request;
  if the drift guard ran in between, use Re-run failed jobs once both repos have it.
- Supply chain: actions are pinned to commit hashes and pip packages to exact versions (`ci/checks.py` fails otherwise);
  Dependabot opens weekly pull requests for newer actions; review, merge into dev and promote as usual. CodeQL scans
  every pull request and push (GitHub Settings -> Code security); its findings are under Security -> Code scanning.
- Job config changed (`config/jobs.yml`, `config/stream.yml`)? Run `python ci/bundle.py --write` and commit
  `databricks.yml` in the same pull request; CI fails otherwise (ADR 0011).
- Token key (ADR 0010): never read, print, copy or replace `lh_vault.<code>_<env>.mask_key`; a new key changes every
  token in that environment. Nobody but the job account gets grants on `lh_vault` (`04_vault_setup` verifies it).

## 5. Routine maintenance
| When | Task | How |
|---|---|---|
| Daily | morning check (section 2) | Developer |
| Monday | read the weekly digest | all |
| Monthly, 1st week | `setup_once/notebook/02_verify_setup` in both repos | Management: 0 FAIL |
| Monthly | `checks/t29_role_access` per role | each person |
| 20 Dec 2026 | rotation day: Git credential `lh_deploy_git_read` (expires 20 Dec), role GitHub tokens (90 days); check the expiry of the CI client secrets | replace in GitHub secrets / Git credentials; then one deploy per env |
| 16 Jan 2027 | admin token in environment `bhg_admin` expires | new Management token, then setup-secret-scopes run |
| On a leak or a leaver | rotate the affected secret at once | `docs/disaster_recovery.md` section 3 |

## 6. Useful checks
`t00_job_smoke` (end of every run), `t28_jobs` (jobs = config), `t29_role_access` (access per person),
`t15_masks_applied`, `t26_bi_views`, `t31_refresh`, `t32_stream`, `t33_dictionary`, `setup_once/notebook/02_verify_setup`.
Alerts by hand: Actions -> alerts -> env, what, mode dry-run (prints, sends nothing).

Note: GitHub's free scheduler may start scheduled workflows late or skip runs at busy times (seen 24 Oct 2026: simulator 3 h late, notices about every 6 h instead of 2 h). Nothing here depends on exact times: alerts and notices are sent once whenever they run, and the simulator chain restores the order source files -> daily job.
