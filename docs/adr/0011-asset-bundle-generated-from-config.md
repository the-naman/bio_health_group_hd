# 0011 Asset Bundle generated from the job config, validated in CI, deployed by our own deployer

- Status: Accepted (Oct 2026)
- Context: Databricks Asset Bundles (`databricks.yml`) are the standard way to describe jobs as code. The jobs here
  were already code (`config/jobs.yml` + `src/lh/jobs.py`, ADR 0004) and are deployed by `ci/deploy.py` as the
  service principal `sp_lh_deploy` into each environment's own Git folder, with failure-mail addresses read from the
  secret scope at deploy time. Moving the deploy to `databricks bundle deploy` would change, on live jobs, the folder
  the notebooks run from, the deployment state and the owner handling, for no gain in what runs.
- Decision: `databricks.yml` (test and prod targets) is generated from the same config by `src/lh/bundle.py`, using
  `jobs.build`, the function the deployer uses, so the two cannot disagree. The job account is looked up by name
  (`variables.job_account.lookup.service_principal`), so no application id is written in Git; mail addresses stay out
  of Git. CI fails when the committed file differs from what the config generates (`ci/checks.py` 7c) and validates it
  against the JSON schema printed by a pinned Databricks CLI (v1.20.0, checksum verified, no login), with the schema's
  `\p{L}` patterns checked by the `regex` package. `bundle validate` itself needs a workspace login, so it is not run.
- Consequences: the bundle is a reviewed, standard description of every test and prod job that a team could deploy
  with `databricks bundle deploy -t <env>` (it would create bundle-managed copies; `bundle deployment bind` would adopt
  the existing jobs). Dev jobs are per person and stay out of the bundle. After a change to `config/jobs.yml` or
  `config/stream.yml`, run `python ci/bundle.py --write` and commit `databricks.yml` with it.
