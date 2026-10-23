# 0001 One config-driven engine per layer

- Status: Accepted (Oct 2026)
- Context: two companies, 24 source tables in 6 formats; a notebook per table would mean 24+ copies of the same logic.
- Decision: Bronze and Silver each have one engine (`src/lh/bronze.py`, `src/lh/silver.py`); every table is one entry in
  `config/tables.yml` and `config/silver.yml`. A `table` setting runs one table on its own (requirement 17).
- Consequences: a new table is a config change plus a check; one bug fix helps every table. The engine must stay generic,
  so company-specific steps live in small extra modules (`prepare.py`, `recon.py`).
