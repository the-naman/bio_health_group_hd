# 0009 Data dictionary as comments in Unity Catalog, from one YAML file

- Status: Accepted (Oct 2026)
- Context: readers of Gold (analysts, Power BI, Genie) need to know what each table and column means, and the meaning
  must not drift from the data or live in a wiki nobody updates.
- Decision: descriptions live in `config/dictionary.yml` next to the code; the BI step of the daily job writes them as
  table, column and view comments (`src/lh/dictionary.py`), after Gold because a full Gold overwrite drops column
  comments. Tests fail when a Gold table or BI view has no description; `checks/t33_dictionary` checks the catalog.
- Consequences: one more file to change with every new Gold column (reviewed in the same PR); a comment that cannot be
  set never stops the daily job, it is reported instead. Descriptions are plain text, one line, no quotes.
