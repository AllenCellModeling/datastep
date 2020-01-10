# step

[![Build Status](https://github.com/AllenCellModeling/step/workflows/Build%20Master/badge.svg)](https://github.com/AllenCellModeling/step/actions)
[![Documentation](https://github.com/AllenCellModeling/step/workflows/Documentation/badge.svg)](https://AllenCellModeling.github.io/step)
[![Code Coverage](https://codecov.io/gh/AllenCellModeling/step/branch/master/graph/badge.svg)](https://codecov.io/gh/AllenCellModeling/step)

A base class and utility functions for creating pure functions steps for DAGs.

---

## Installation
**Development Head:** `pip install git+https://github.com/AllenCellModeling/step.git`

## Documentation
For full package documentation please visit [AllenCellModeling.github.io/step](https://AllenCellModeling.github.io/step).

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.

#### Additional Optional Setup Steps:
* Turn your project into a GitHub repository:
  * Make sure you have `git` installed, if you don't, [follow these instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
  * Make an account on [github.com](https://github.com)
  * Go to [make a new repository](https://github.com/new)
  * _Recommendations:_
    * _It is strongly recommended to make the repository name the same as the Python package name_
    * _A lot of the following optional steps are *free* if the repository is Public, plus open source is cool_
  * Once you are in your newly generated cookiecutter Python project directory, run `git init`
  * After `git` has initialized locally, run the following commands:
    * `git remote add origin git@github.com:AllenCellModeling/step.git`
    * `git push -u origin master`
* Register step with Codecov:
  * Make an account on [codecov.io](https://codecov.io) (Recommended to sign in with GitHub)
  * Select `AllenCellModeling` and click: `Add new repository`
  * Copy the token provided, go to your [GitHub repository's settings and under the `Secrets` tab](https://github.com/AllenCellModeling/step/settings/secrets),
  add a secret called `CODECOV_TOKEN` with the token you just copied.
  Don't worry, no one will see this token because it will be encrypted.
* Generate and add an access token as a secret to the repository for auto documentation generation to work
  * Go to your [GitHub account's Personal Access Tokens page](https://github.com/settings/tokens)
  * Click: `Generate new token`
  * _Recommendations:_
    * _Name the token: "Auto-Documentation Generation" or similar so you know what it is being used for later_
    * _Select only: `repo:status`, `repo_deployment`, and `public_repo` to limit what this token has access to_
  * Copy the newly generated token
  * Go to your [GitHub repository's settings and under the `Secrets` tab](https://github.com/AllenCellModeling/step/settings/secrets),
  add a secret called `ACCESS_TOKEN` with the personal access token you just created.
  Don't worry, no one will see this password because it will be encrypted.
* Register your project with PyPI:
  * Make an account on [pypi.org](https://pypi.org)
  * Go to your [GitHub repository's settings and under the `Secrets` tab](https://github.com/AllenCellModeling/step/settings/secrets),
  add a secret called `PYPI_TOKEN` with your password for your PyPI account.
  Don't worry, no one will see this password because it will be encrypted.
  * Next time you push to the branch: `stable`, GitHub actions will build and deploy your Python package to PyPI.
  * _Recommendation: Prior to pushing to `stable` it is recommended to install and run `bumpversion` as this will,
  tag a git commit for release and update the `setup.py` version number._
* Add branch protections to `master` and `stable`
    * To protect from just anyone pushing to `master` or `stable` (the branches with more tests and deploy
    configurations)
    * Go to your [GitHub repository's settings and under the `Branches` tab](https://github.com/AllenCellModeling/step/settings/branches), click `Add rule` and select the
    settings you believe best.
    * _Recommendations:_
      * _Require pull request reviews before merging_
      * _Require status checks to pass before merging (Recommended: lint and test)_


***Free software: Allen Institute Software License***
