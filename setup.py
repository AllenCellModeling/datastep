#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

setup_requirements = [
    "pytest-runner>=5.2",
]

test_requirements = [
    "black>=19.10b0",
    "codecov>=2.1.4",
    "flake8>=3.8.3",
    "flake8-debugger>=3.2.1",
    "pytest>=5.4.3",
    "pytest-cov>=2.9.0",
    "pytest-raises>=0.11",
]

dev_requirements = [
    *setup_requirements,
    *test_requirements,
    "bumpversion>=0.6.0",
    "coverage>=5.1",
    "ipython>=7.15.0",
    "m2r>=0.2.1",
    "pytest-runner>=5.2",
    "Sphinx>=2.0.0b1,<3",
    "sphinx_rtd_theme>=0.4.3",
    "tox>=3.15.2",
    "twine>=3.1.1",
    "wheel>=0.34.2",
]

requirements = [
    "boto3>=1.11.9",
    "botocore>=1.15.0",
    "docutils<0.16",
    "gitpython>=3.0.5",
    "jinja2>=2.10.3",
    "pandas",
    "prefect",
    "python-dateutil<=2.8.0",
    "quilt3>=3.1.10",
    "urllib3<1.25",  # quilt conflict
    "tqdm",
]

extra_requirements = {
    "setup": setup_requirements,
    "test": test_requirements,
    "dev": dev_requirements,
    "all": [
        *requirements,
        *dev_requirements,
    ]
}

setup(
    author="Jackson Maxfield Brown",
    author_email="jacksonb@alleninstitute.org",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: Free for non-commercial use",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description=(
        "A base class and utilities for creating steps in DAGs that are tied "
        "to large amounts of data."
    ),
    entry_points={"console_scripts": ["make_new_step=datastep.bin.make_new_step:main"]},
    install_requires=requirements,
    license="Allen Institute Software License",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="datastep, DAG, pipeline, workflow",
    name="datastep",
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*"]),
    python_requires=">=3.6",
    setup_requires=setup_requirements,
    test_suite="datastep/tests",
    tests_require=test_requirements,
    extras_require=extra_requirements,
    url="https://github.com/AllenCellModeling/datastep",
    # Do not edit this string manually, always use bumpversion
    # Details in CONTRIBUTING.rst
    version="0.1.6",
    zip_safe=False,
)
