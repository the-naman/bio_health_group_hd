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
| `checks/` | automated checks (t00 = smoke check of every daily run) |
| `ci/`, `.github/workflows/` | promotion checks, approval notices, deploy after a merge |
| `config/` | every setting, per environment. No secrets: they live in GitHub secrets and Databricks secret scopes |
| `setup_once/` | one-time environment setup and access grants (admin) |

## How code moves
- Work happens on `bhg_dev`: each person runs their own dev job (`lh_bhg_dev_daily_<role>`).
- `bhg_dev` -> `bhg_test` and `bhg_test` -> `bhg_prod` only by pull request. Checks run first; the Manager
  approves test, the Business-user Manager approves prod. A rejection sends the change back to the lower branch.
- After a merge, CI pulls the environment's Git folder, redeploys the daily job and checks it (`checks/t28_jobs`).
- Only code moves between environments, never data. No personal data, address or secret is ever committed.
