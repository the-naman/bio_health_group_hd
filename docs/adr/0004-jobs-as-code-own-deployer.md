# 0004 Jobs as code with our own deployer and job accounts

- Status: Accepted (Oct 2026); Databricks Asset Bundles to be added as a validated artifact (roadmap item 11)
- Context: Free Edition, serverless only; test/prod jobs must run as least-privilege job accounts, deployed by CI.
- Decision: `config/jobs.yml` + `src/lh/jobs.py` build each job; `ci/deploy.py` runs as service principal `sp_lh_deploy`
  (no data access), pulls the environment's Git folder, redeploys and checks it with `checks/t28_jobs`.
- Consequences: full control and a drift check on every deploy. A brand-new test/prod job needs Management to set its
  owner once (only workspace admins may change a job owner); this is in the runbook.
