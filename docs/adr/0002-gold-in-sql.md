# 0002 Gold written as one SQL file per table

- Status: Accepted (14 Oct 2026; replaced the PySpark Gold builders)
- Context: business users and managers read SQL, not PySpark; Gold logic must be reviewable line by line.
- Decision: each Gold table is one SELECT in `gold/sql/<table>.sql`, run in the order of `config/gold.yml` by
  `src/lh/gold_sql.py`, which passes named parameters and keeps masks and row filters on save.
- Consequences: parity with the old builders proven row for row; runs became about 8 minutes faster. Python is kept only
  where SQL cannot do the work (the keyed fingerprint function).
