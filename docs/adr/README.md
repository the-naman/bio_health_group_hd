# Decision records

Short records of the decisions that shape this platform (same set in both repos). Format: context, decision,
consequences. A decision is changed by a new record that replaces the old one, never by editing history.

| # | Decision | Status |
|---|---|---|
| 0001 | One config-driven engine per layer, one config entry per table | Accepted |
| 0002 | Gold written as one SQL file per table | Accepted |
| 0003 | Personal data protected by Unity Catalog masks and row filters | Accepted |
| 0004 | Jobs as code with our own deployer and job accounts | Accepted |
| 0005 | Code moves by approved pull requests; data never moves | Accepted |
| 0006 | Real-time stream as scheduled availableNow runs | Accepted |
| 0007 | Two repos sharing one framework | Accepted |
| 0008 | Alerts and notices from GitHub Actions, each finding once | Accepted |
| 0009 | Data dictionary as comments in Unity Catalog, from one YAML file | Accepted |
| 0010 | Keyed token masks, with the key in a vault catalog | Accepted |
| 0011 | Asset Bundle generated from the job config, validated in CI, deployed by our own deployer | Accepted |
