# 0008 Alerts and notices from GitHub Actions, each finding once

- Status: Accepted (Oct 2026)
- Context: people should get only mails they act on, and never the same finding twice.
- Decision: scheduled workflows run `ci/alerts.py` and `ci/notices.py` as service principal `sp_lh_notices` (read-only on
  health views); every finding is keyed in `ops.notice_log`; recipients by role from config, addresses only in secrets.
- Consequences: no Databricks SQL alert setup per environment; schedules avoid the top of the hour, where GitHub may delay
  or drop runs.
