# datastep

[![Build Status](https://github.com/AllenCellModeling/datastep/workflows/Build%20Master/badge.svg)](https://github.com/AllenCellModeling/datastep/actions)
[![Documentation](https://github.com/AllenCellModeling/datastep/workflows/Documentation/badge.svg)](https://AllenCellModeling.github.io/datastep)
[![Code Coverage](https://codecov.io/gh/AllenCellModeling/datastep/branch/master/graph/badge.svg)](https://codecov.io/gh/AllenCellModeling/datastep)

Base class and utility functions for creating data-centric steps for workflow DAGs, with the idea that each step is heavily tied to large amounts of
data.

---

This library should rarely be used by itself, it was developed in pair with
[cookiecutter-stepworkflow](https://github.com/AllenCellModeling/cookiecutter-stepworkflow)
and you should look there for more context-rich documentation.

## Installation
**Stable Release:**

`pip install datastep`

**Development Head:**

`pip install git+https://github.com/AllenCellModeling/datastep.git`

## Documentation
For full package documentation please visit
[AllenCellModeling.github.io/datastep](https://AllenCellModeling.github.io/datastep).

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the
code (from getting started to e.g. using bumpversion to make a new release).

***Free software: Allen Institute Software License***
