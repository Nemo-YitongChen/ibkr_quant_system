# IBKR Quant System

A quantitative trading system built around the Interactive Brokers ecosystem, designed to support research, configuration-driven execution, and future production hardening.

## Overview
This repository contains the foundation of a Python-based quantitative trading workflow. It is intended to separate configuration, market connectivity, trading logic, and operational controls so the system can evolve in a maintainable way.

## Goals
- Build a structured research-to-execution workflow
- Keep trading logic modular and configurable
- Improve reproducibility, safety, and observability
- Prepare the project for testing and deployment

## Tech Stack
- Python
- pandas
- PyYAML
- ib_insync

## Current Structure
The project should gradually move toward a structure similar to:

```text
src/
  core/
  data/
  execution/
  risk/
  strategies/
config/
docs/
reports/
tests/
```

## Getting Started
### Prerequisites
- Python 3.10+
- Interactive Brokers account and API access
- TWS or IB Gateway configured locally

### Installation
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run
Document the main entry point and required configuration before executing live or paper-trading workflows.

## Configuration
Use environment variables and configuration files to separate credentials, broker settings, symbols, and strategy parameters.

## Roadmap
- Add environment variable documentation
- Add architecture and data flow documentation
- Add tests for config parsing and core trading logic
- Add CI checks and release hygiene

## Notes
This repository should include a clear distinction between paper trading, backtesting, and any future live execution modes.

## License
Add a license file if you plan to open-source the project.
