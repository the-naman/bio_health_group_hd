# 0005 Code moves by approved pull requests; data never moves

- Status: Accepted (Oct 2026)
- Context: requirements 7, 9 and 13: isolated data per environment, approvals for test and prod, rejected changes loop back.
- Decision: dev -> test (Manager) and test -> prod (Business Manager) only by pull request with 8 automatic checks and a
  GitHub environment approval; hotfixes branch from prod and are merged back to dev automatically. Test data comes only
  from the controlled, sanitised refresh.
- Consequences: every prod change is reviewed and traceable to a commit; branch rulesets block direct pushes.
