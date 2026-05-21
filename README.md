# hb-event-bus

<!-- [![CI](https://github.com/MementoRC/hb-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/MementoRC/hb-event-bus/actions/workflows/ci.yml) -->
<!-- [![codecov](https://codecov.io/gh/MementoRC/hb-event-bus)](https://codecov.io/gh/MementoRC/hb-event-bus) -->
<!-- [![PyPI version](https://badge.fury.io/py/hb-event-bus.svg)](https://badge.fury.io/py/hb-event-bus) -->
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Event bus for asynchronous inter-component messaging in Hummingbot sub-packages.

> **Scaffold note**: This package is in initial scaffolding state. The Python module structure,
> CI, and quality gates are in place. Feature implementation is tracked in follow-up PRs.

## Installation

```bash
pip install hb-event-bus
```

Or with pixi:

```bash
pixi add hb-event-bus
```

## Usage

```python
import event_bus

print(event_bus.__version__)
```

## Development

```bash
# Install dev dependencies
pixi install

# Run tests
pixi run test

# Run quality checks
pixi run quality

# Run full check suite
pixi run check
```

## Sibling Dependencies

Runtime dependencies on `hb-async-utils` and `hb-data-type-primitives` are deferred to a
follow-up feature PR. These siblings must be published to PyPI (or referenced via git+https)
before they can be declared as install dependencies.
