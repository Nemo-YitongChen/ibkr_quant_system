# Project Overview

## Positioning
ibkr_quant_system is a quantitative trading system built around the Interactive Brokers ecosystem.

## Goals
- Provide a maintainable research-to-execution workflow
- Separate configuration, strategy logic, execution, and reporting
- Improve reproducibility, safety checks, and future deployment readiness

## Current Signals From The Repository
- Python dependency management is currently based on `requirements.txt`
- The repository needs richer setup, architecture, and environment documentation

## Recommended Structure
```text
src/
  core/
  strategies/
  execution/
  risk/
  data/
config/
notebooks/
reports/
tests/
docs/
```

## Immediate Next Steps
1. Document environment variables and broker connectivity requirements
2. Add architecture and strategy lifecycle documentation
3. Introduce tests for configuration parsing and core trading logic
4. Add CI checks for formatting, linting, and test execution

## Notes
This file was added as part of repository standardization so the project can be understood quickly by collaborators and reviewers.
