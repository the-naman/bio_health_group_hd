## What changes
<!-- One or two lines: what and why. Name the requirement or audit item if there is one. -->

## Promotion path
- [ ] dev -> test (Manager approves)
- [ ] test -> prod (Business Manager approves)
- [ ] hotfix (`<code>_hotfix_<name>` from prod; urgent approval)

## Checked before asking for review
- [ ] My own dev job ran green (smoke check 0 FAIL)
- [ ] The checks for what I changed pass in dev (t-number: ...)
- [ ] `python ci/checks.py` passes (compile, no outputs, YAML, jobs, dashboards, no email or token, ruff)
- [ ] Shared files changed in both repos the same way (src/lh, ci, simulator, workflows)
- [ ] No personal data, email address, token or secret in code, config, notebook output or commit message
- [ ] README, CHANGELOG or a decision record updated if behaviour changed

## After merge
- [ ] Deploy green (`deploy_job` and `check_jobs` SUCCESS)
- [ ] Prod only: back-merge check green
