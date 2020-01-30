#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from datastep import file_utils
from .test_step import Test

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
    test = Test()
    test.run(N=N)
    df_abs = test.manifest.copy()

    file_utils.manifest_filepaths_abs2rel(test)
    file_utils.manifest_filepaths_rel2abs(test)

    assert (
        df_abs["filepath"].astype(str) == test.manifest["filepath"].astype(str)
    ).all()


def test_2Xabs2rel2abs(N=3):
    test = Test()
    test.run(N=N)
    df_abs = test.manifest.copy()

    file_utils.manifest_filepaths_abs2rel(test)
    file_utils.manifest_filepaths_abs2rel(test)
    file_utils.manifest_filepaths_rel2abs(test)
    file_utils.manifest_filepaths_rel2abs(test)

    assert (
        df_abs["filepath"].astype(str) == test.manifest["filepath"].astype(str)
    ).all()
