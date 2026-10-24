# Changelog

Notable changes to this platform, newest first (same file in both repos). Format based on Keep a Changelog; dates in IST.

## Unreleased (dev)
### Added
- Repo standards: ruff lint and format (`pyproject.toml`) enforced by `ci/checks.py`, PR template, CODEOWNERS,
  `.editorconfig`, decision records in `docs/adr/`, this changelog.
- Tests in the repo (`tests/`, pytest): config rules, simulator determinism and end-to-end scenarios on local Spark
  (mail routing, dev activity, alerts, new-column watch, stream logic and views); run on every pull request.
- Shared-code drift guard (`ci/shared.py`, rules in `config/ci.yml` -> shared): every pull request compares the shared
  files with the other repo's branch for the same environment and fails on any difference or undeclared file.
- Data dictionary (`config/dictionary.yml`, `src/lh/dictionary.py`): every Silver table, Gold table and column and BI
  view carries a plain-language description in Unity Catalog (Catalog Explorer, Genie, Power BI). Set by the BI step
  after Gold, only where it changed; checked by `checks/t33_dictionary` and the tests.
- Payment stream health alert (FQF, `ci/alerts.py`, rules `config/alerts.yml` -> stream_health): one mail when the
  stream stops finishing runs, runs without Kafka events, or leaves out an unusual share of messages; weekly digest
  line with stream runs and events. Failed stream tasks are still mailed by Databricks.
- Operations runbook (`docs/runbook.md`) and disaster-recovery note (`docs/disaster_recovery.md`: RPO/RTO per asset,
  scenarios, known gaps, drills).
- Access matrix in one place (`src/lh/access.py`), used by `01_grant_access` and the refreshed read-only
  `02_verify_setup` (groups, admins, service principals, grants incl. refresh, volumes, secret scopes, own tokens).
- Performance (`docs/performance.md`, `config/performance.yml`): Bronze loads 4 tables side by side; the BI step
  compacts small-file tables (run logs, payment stream) only when needed.
- Gold column descriptions are written with the data (`gold.save`), so the BI step no longer sets them again every run.
- Simulator chain (`ci/chain.py`, chain job in `simulator.yml`): when GitHub starts the simulator after the daily job
  already ran, the job runs again for the new files (FQF, then BHG for the group view); never twice for the same files.
- Smoke check WARN when a test/prod run loads no new source files.
- Supply chain: GitHub actions pinned to commit hashes, pip packages to exact versions, checked by `ci/checks.py`;
  Dependabot (`.github/dependabot.yml`) proposes action updates weekly; CodeQL code scanning on.
- `checks/t29_role_access` reads its expectations from the access matrix (`src/lh/access.py`).
### Fixed
- `ci/flow.py` asks GitHub up to 3 times (busy, network, merge not linked yet) before mailing a deploy as a promotion.
- `01_grant_access` prints job-account names, never their application ids.

## 2026-10-23 - go-live and real-time stream
### Added
- 5.13 go-live: schedules on (prod FQF 03:00, BHG 04:00; test 05:00, 06:00 IST), simulator 00:01 with a 01:01 safety run.
- 5.14 real-time payment events (FQF): Kafka -> Bronze -> Silver (watermark, de-duplication) -> Gold failure windows;
  velocity alert by mail; dashboard page "Payment events"; Gold customer reference masked in test/prod.
- README section "Go-live: schedule and operations".
### Fixed
- Failed deploys mail the fixers and Management only, never the Business Manager.
- Prod alerts moved after the prod jobs (04:45 to 20:45 IST); test alerts daily at 07:05 IST.
- Audit-row timestamps use timezone-aware UTC.

## 2026-10-22 - controlled refresh and observability
### Added
- 5.11 controlled refresh prod -> test and test -> dev: people sampled by keyed hash, personal values faked, ids re-keyed.
- 5.12 alerts (refused files, Silver rebuild, quarantine spike, new source column, merge backlog, over-limit) and the
  Monday digest, each finding mailed once.
### Fixed
- Dev roles get MANAGE on their dev catalog, so any of them can run the whole dev job.

## 2026-10-21 - first prod release
### Added
- Promotion test -> prod with Business Manager approval; prod job accounts; emergency hotfix path with back-merge.

## 2026-10-14 to 2026-10-18 - promotion and safety
### Added
- Gold rewritten as one SQL file per table; CI deploys as service principal; business notices; role access check;
  new source columns flow through with a backfill; single-table runs.
### Security
- Commit history cleaned of two early commit-metadata email addresses; admin token moved behind an approval.

## 2026-09-27 to 2026-10-13 - foundation
### Added
- Environments, catalogs, access matrix, synthetic data simulator, Bronze, Silver (SCD2, quarantine, reconciliation,
  OCR), Gold, group consolidation, masks and row filter, BI views and dashboards, jobs as code.
