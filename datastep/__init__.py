# -*- coding: utf-8 -*-

"""Top-level package for datastep."""

__author__ = "Jackson Maxfield Brown"
__email__ = "jacksonb@alleninstitute.org"
# Do not edit this string manually, always use bumpversion
# Details in CONTRIBUTING.md
__version__ = "0.1.8"


def get_module_version():
    return __version__


from .step import Step, log_run_params  # noqa: F401
