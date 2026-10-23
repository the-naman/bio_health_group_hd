# 0007 Two repos sharing one framework

- Status: Accepted (Oct 2026)
- Context: requirement 22: the child company must be easy to separate later.
- Decision: one repo per company (`bio_health_group_hd`, `flexi_quant_finance_fd`) with identical shared code (`src/lh`,
  `ci`, `simulator`, most workflows); only config, Gold SQL, dashboards and checks differ.
- Consequences: separating the child is handing over one repo. The shared code is kept identical by hand today; a CI check
  that fails on any difference is planned (roadmap item 5), a shared package is the later option.
