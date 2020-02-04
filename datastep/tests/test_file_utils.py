#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from datastep import file_utils

from .example_step import ExampleStep

###############################################################################


@pytest.mark.parametrize(
    "f",
    [
        ("example_config_1.json"),
        pytest.param(
            "example_directory", marks=pytest.mark.raises(exceptions=IsADirectoryError)
        ),
        pytest.param(
            "not_a_file.json", marks=pytest.mark.raises(exceptions=FileNotFoundError)
        ),
    ],
)
def test_resolve_filepath(data_dir, f):
    # Route to data dir
    f = data_dir / f

    # Run
    file_utils.resolve_filepath(f)


@pytest.mark.parametrize(
    "f",
    [
        (None),
        pytest.param(
            "example_config_1.json",
            marks=pytest.mark.raises(exceptions=FileExistsError),
        ),
    ],
)
def test_resolve_directory(data_dir, f):
    if f is None:
        f = data_dir
    else:
        # Route to data dir
        f = data_dir / f

    # Run
    file_utils.resolve_directory(f)


def test_abs2rel2abs(N=3):
    example_step = ExampleStep()
    example_step.run(N=N)
    df_abs = example_step.manifest.copy()

    file_utils.manifest_filepaths_abs2rel(example_step)
    file_utils.manifest_filepaths_rel2abs(example_step)

    assert (
        df_abs["filepath"].astype(str) == example_step.manifest["filepath"].astype(str)
    ).all()


def test_2Xabs2rel2abs(N=3):
    example_step = ExampleStep()
    example_step.run(N=N)
    df_abs = example_step.manifest.copy()

    file_utils.manifest_filepaths_abs2rel(example_step)
    file_utils.manifest_filepaths_abs2rel(example_step)
    file_utils.manifest_filepaths_rel2abs(example_step)
    file_utils.manifest_filepaths_rel2abs(example_step)

    assert (
        df_abs["filepath"].astype(str) == example_step.manifest["filepath"].astype(str)
    ).all()


def test_sanitize_name():
    output_str = file_utils._sanitize_name("my dir")
    assert output_str == "my_dir"
