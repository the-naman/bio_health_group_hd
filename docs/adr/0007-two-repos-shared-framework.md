# 0007 Two repos sharing one framework

- Status: Accepted (Oct 2026)
- Context: requirement 22: the child company must be easy to separate later.
- Decision: one repo per company (`bio_health_group_hd`, `flexi_quant_finance_fd`) with identical shared code (`src/lh`,
  `ci`, `simulator`, most workflows); only config, Gold SQL, dashboards and checks differ.
- Consequences: separating the child is handing over one repo. Every pull request runs `ci/shared.py`, which compares
  the shared files with the other repo's branch for the same environment and fails on any difference or on a file that
  exists in one repo without being declared company-only (`config/ci.yml` -> shared). A shared package is the later option.
