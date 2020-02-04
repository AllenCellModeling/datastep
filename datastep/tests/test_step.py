#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from pathlib import Path

import pytest

from datastep import constants, file_utils

from .example_step import ExampleStep

###############################################################################


@pytest.mark.parametrize(
    "config_var, "
    "set_env, "
    "use_cwd, "
    "direct_upstream_tasks, "
    "expected_direct_upstream_tasks, "
    "expected_storage_bucket, "
    "expected_project_local_staging_dir, "
    "expected_step_local_staging_dir",
    [
        # Provided none, use defaults
        (
            None,
            None,
            None,
            [],
            [],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="examplestep"
                ),
                make=True,
            ),
        ),
        # Provided only config var, use it
        (
            "example_config_1.json",
            None,
            None,
            [],
            [],
            "s3://example_config_1",
            "example/config/1",
            "example/config/1/examplestep",
        ),
        # Provided config var and env exists, use config var
        (
            "example_config_1.json",
            "example_config_2.json",
            None,
            [],
            [],
            "s3://example_config_1",
            "example/config/1",
            "example/config/1/examplestep",
        ),
        # Provided all options, use config var
        (
            "example_config_1.json",
            "example_config_2.json",
            "example_config_3.json",
            [],
            [],
            "s3://example_config_1",
            "example/config/1",
            "example/config/1/examplestep",
        ),
        # Provided config var and current working directory, use config var
        (
            "example_config_1.json",
            None,
            "example_config_3.json",
            [],
            [],
            "s3://example_config_1",
            "example/config/1",
            "example/config/1/examplestep",
        ),
        # Provided env and current working directory, use env
        (
            None,
            "example_config_2.json",
            "example_config_3.json",
            [],
            [],
            "s3://example_config_2",
            "example/config/2",
            "example/config/2/examplestep",
        ),
        # Provided env, use env
        (
            None,
            "example_config_2.json",
            None,
            [],
            [],
            "s3://example_config_2",
            "example/config/2",
            "example/config/2/examplestep",
        ),
        # Provided current working directory, use current working directory
        (
            None,
            None,
            "example_config_3.json",
            None,
            [],
            "s3://example_config_3",
            "example/config/3",
            "example/config/3/examplestep",
        ),
        # Missing bucket value from config
        (
            "example_config_4.json",
            None,
            None,
            [],
            [],
            constants.DEFAULT_QUILT_STORAGE,
            "example/config/4",
            "example/config/4/examplestep",
        ),
        # Missing local storage value from config
        (
            "example_config_5.json",
            None,
            None,
            [],
            [],
            "s3://example_config_5",
            file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="examplestep"
                ),
                make=True,
            ),
        ),
        # Missing both values from config
        # Lol why use a config
        (
            "example_config_6.json",
            None,
            None,
            [],
            [],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="examplestep"
                ),
                make=True,
            ),
        ),
        # Specific output directory for step available
        (
            "example_config_7.json",
            None,
            None,
            [],
            [],
            "s3://example_config_7",
            "example/config/7",
            "example/config/7/examplestep",
        ),
        # Step has key in config but no specific output directory listed
        (
            "example_config_8.json",
            None,
            None,
            [],
            [],
            "s3://example_config_8",
            "example/config/8",
            "example/config/8/examplestep",
        ),
        # Checking upstream tasks
        (
            None,
            None,
            [],
            ["raw", "qc", "norm"],
            ["raw", "qc", "norm"],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="examplestep"
                ),
                make=True,
            ),
        ),
        # Checking when config doesn't exist
        pytest.param(
            "config_does_not_exist.json",
            None,
            None,
            [],
            [],
            None,
            None,
            None,
            marks=pytest.mark.raises(exceptions=FileNotFoundError),
        ),
    ],
)
def test_init(
    tmpdir,
    data_dir,
    config_var,
    set_env,
    use_cwd,
    direct_upstream_tasks,
    expected_direct_upstream_tasks,
    expected_storage_bucket,
    expected_project_local_staging_dir,
    expected_step_local_staging_dir,
):
    # Set up init configuration with config var
    if isinstance(config_var, str):
        config_var = str(data_dir / config_var)

    # Set up init configuration with environment
    if set_env is not None:
        os.environ[constants.CONFIG_ENV_VAR_NAME] = str(data_dir / set_env)

    # Set up init configuration with current working directory file
    if use_cwd is not None:
        original_dir = Path().expanduser().resolve()
        shutil.copyfile(
            data_dir / use_cwd, Path(tmpdir) / constants.CWD_CONFIG_FILE_NAME
        )
        os.chdir(tmpdir)

    # Run init
    t = ExampleStep(direct_upstream_tasks=direct_upstream_tasks, config=config_var)

    # Not required but may as well run it
    t.run()

    # Check init
    assert t.step_name == "examplestep"
    assert t.upstream_tasks == expected_direct_upstream_tasks
    assert t.storage_bucket == expected_storage_bucket
    assert str(Path(expected_project_local_staging_dir)) in str(
        t.project_local_staging_dir
    )
    assert str(Path(expected_step_local_staging_dir)) in str(t.step_local_staging_dir)

    # Clear env
    os.environ.pop(constants.CONFIG_ENV_VAR_NAME, None)

    # Reset current working directory
    if use_cwd is not None:
        os.chdir(original_dir)


def test_run():
    t = ExampleStep()
    t.run()
    assert len([file for file in t.step_local_staging_dir.iterdir()]) > 0


def test_clean():
    t = ExampleStep()
    t.run()
    t.clean()
    assert len([file for file in t.step_local_staging_dir.iterdir()]) == 0
