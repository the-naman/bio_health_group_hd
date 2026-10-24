# Security policy

This is a portfolio lakehouse built on synthetic data: no real people, patients or customers are in it.

## Reporting a problem
Please do not open a public issue. Use **Security -> Report a vulnerability** on this repository (GitHub private
vulnerability reporting). You will get an answer within 7 days.

Useful to include: what you found, where (file or workflow), and how to see it. Please do not test against the
Databricks workspace, the AWS account or the Kafka service; reading the code is enough.

## In scope
- Code and workflows in this repository (`src/`, `ci/`, `.github/workflows/`, notebooks).
- A secret, token, key or personal detail committed by mistake.
- A way around the access design: masks, row filters, job accounts, approvals (`docs/adr/0003`, `0005`, `0010`).

## How the project protects itself
- Secrets only in GitHub secrets and Databricks secret scopes; secret scanning with push protection is on.
- Actions pinned to commit hashes, packages to exact versions, Dependabot and CodeQL on (`ci/checks.py`).
- Code reaches test and prod only through an approved pull request; jobs run as service principals.
