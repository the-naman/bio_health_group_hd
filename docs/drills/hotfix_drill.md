# 10j hotfix drill

Harmless change used to prove the emergency hotfix path end to end:

1. branch `bhg_hotfix_drill` from `bhg_prod`
2. pull request into `bhg_test`: route "hotfix", environment `bhg_test_hotfix`, URGENT mail, merge commit, deploy
3. after go-live (10h): pull request into `bhg_prod`, then the automatic back-merge pull request `bhg_prod` -> `bhg_dev`
