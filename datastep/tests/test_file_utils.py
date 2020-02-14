#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import pandas as pd
import pytest

from datastep import file_utils

###############################################################################


@pytest.mark.parametrize(
    "f, strict",
    [
        ("example_config_1.json", True),
        ("example_config_1.json", False),
        pytest.param(
            "example_directory",
            True,
            marks=pytest.mark.raises(exceptions=IsADirectoryError),
        ),
        pytest.param(
            "not_a_file.json",
            True,
            marks=pytest.mark.raises(exceptions=FileNotFoundError),
        ),
        pytest.param(
            "not_a_file.json",
            False,
            marks=pytest.mark.raises(exceptions=FileNotFoundError),
        ),
    ],
)
def test_resolve_filepath(data_dir, f, strict):
    # Route to data dir
    f = data_dir / f

    # Run
    result = file_utils.resolve_filepath(f, strict)

    # Assert expected value based off strict
    if strict:
        assert result.is_absolute()
    else:
        assert str(result) == str(f)


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


@pytest.mark.parametrize(
    "manifest, filepath_columns, relative_dir",
    [
        (
            pd.DataFrame(
                [
                    {"filepath": "example_config_1.json"},
                    {"filepath": "example_config_2.json"},
                    {"filepath": "example_config_3.json"},
                ]
            ),
            ["filepath"],
            Path(__file__).parent / "data",
        ),
        (
            pd.DataFrame(
                [
                    {"files": "example_config_1.json"},
                    {"files": "example_config_2.json"},
                    {"files": "example_config_3.json"},
                ]
            ),
            ["files"],
            Path(__file__).parent / "data",
        ),
        (
            pd.DataFrame(
                [
                    {
                        "filepath": "example_config_1.json",
                        "files": "example_config_2.json",
                    },
                    {
                        "filepath": "example_config_3.json",
                        "files": "example_config_4.json",
                    },
                    {
                        "filepath": "example_config_5.json",
                        "files": "example_config_6.json",
                    },
                ]
            ),
            ["filepath", "files"],
            Path(__file__).parent / "data",
        ),
    ],
)
def test_rel2abs2rel(manifest, filepath_columns, relative_dir):
    # Run rel2abs
    df_abs = file_utils.manifest_filepaths_rel2abs(
        manifest, filepath_columns, relative_dir,
    )

    # Run abs2rel
    df_rel = file_utils.manifest_filepaths_abs2rel(
        df_abs, filepath_columns, relative_dir,
    )

    # Check that the paths in each filepath column are equal to the original manifest
    for col in filepath_columns:
        assert (df_rel[col].astype(str) == manifest[col].astype(str)).all()


@pytest.mark.parametrize(
    "manifest, filepath_columns, relative_dir",
    [
        (
            pd.DataFrame(
                [
                    {"filepath": "example_config_1.json"},
                    {"filepath": "example_config_2.json"},
                    {"filepath": "example_config_3.json"},
                ]
            ),
            ["filepath"],
            Path(__file__).parent / "data",
        ),
        (
            pd.DataFrame(
                [
                    {"files": "example_config_1.json"},
                    {"files": "example_config_2.json"},
                    {"files": "example_config_3.json"},
                ]
            ),
            ["files"],
            Path(__file__).parent / "data",
        ),
        (
            pd.DataFrame(
                [
                    {
                        "filepath": "example_config_1.json",
                        "files": "example_config_2.json",
                    },
                    {
                        "filepath": "example_config_3.json",
                        "files": "example_config_4.json",
                    },
                    {
                        "filepath": "example_config_5.json",
                        "files": "example_config_6.json",
                    },
                ]
            ),
            ["filepath", "files"],
            Path(__file__).parent / "data",
        ),
    ],
)
def test_2Xrel2abs2rel(manifest, filepath_columns, relative_dir):
    # Run rel2abs
    df_abs = file_utils.manifest_filepaths_rel2abs(
        manifest, filepath_columns, relative_dir,
    )

    # Run rel2abs round two
    df_abs_2 = file_utils.manifest_filepaths_rel2abs(
        df_abs, filepath_columns, relative_dir,
    )

    # Run abs2rel
    df_rel = file_utils.manifest_filepaths_abs2rel(
        df_abs_2, filepath_columns, relative_dir,
    )

    # Run abs2rel round two
    df_rel_2 = file_utils.manifest_filepaths_abs2rel(
        df_rel, filepath_columns, relative_dir,
    )

    # Check that the paths in each filepath column are equal to the original manifest
    for col in filepath_columns:
        assert (df_rel_2[col].astype(str) == manifest[col].astype(str)).all()


def test_sanitize_name():
    output_str = file_utils._sanitize_name("my dir")
    assert output_str == "my_dir"
