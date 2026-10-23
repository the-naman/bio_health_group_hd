# 0003 Personal data protected by Unity Catalog masks and row filters

- Status: Accepted (Oct 2026)
- Context: real values are needed by the pipeline and by Management, never by developers or business users in test/prod.
- Decision: one file `config/privacy.yml` drives column masks (last4, initial, hide, token) and row filters; ids become
  consistent tokens in test/prod; raw files are closed in test/prod. Data is never copied in a masked form.
- Consequences: one copy of the data, access decided per reader. Masked tables refuse time travel and RESTORE, so Silver
  rebuilds from Bronze instead. A table that is a streaming source cannot carry a mask (see 0006).
