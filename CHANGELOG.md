# Changelog

Notable changes to this platform, newest first (same file in both repos). Format based on Keep a Changelog; dates in IST.

## Unreleased (dev)
### Added
- Repo standards: ruff lint and format (`pyproject.toml`) enforced by `ci/checks.py`, PR template, CODEOWNERS,
  `.editorconfig`, decision records in `docs/adr/`, this changelog.
- Tests in the repo (`tests/`, pytest): config rules, simulator determinism and end-to-end scenarios on local Spark
  (mail routing, dev activity, alerts, new-column watch, stream logic and views); run on every pull request.

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
