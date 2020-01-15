#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from pathlib import Path

import pytest
from step import Step, constants, file_utils

###############################################################################


# A dummy class to test with
class Test(Step):
    def __init__(self, direct_upstream_tasks=None, config=None):
        super().__init__(direct_upstream_tasks, config)

    def run(self, a=1):
        print(a)


###############################################################################


@pytest.mark.parametrize(
    "config_var, "
    "set_env, "
    "use_cwd, "
    "direct_upstream_tasks, "
    "expected_direct_upstream_tasks, "
    "expected_storage_bucket, "
    "expected_local_storage",
    [
        # Provided none, use defaults
        (
            None,
            None,
            None,
            None,
            [],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_LOCAL_TEMP_OUTPUTS.format(cwd=".", module_name="test")
            ),
        ),
        # Provided only config var, use it
        (
            "example_config_1.json",
            None,
            None,
            None,
            [],
            "s3://example_config_1",
            "example/config/1",
        ),
        # Provided config var and env exists, use config var
        (
            "example_config_1.json",
            "example_config_2.json",
            None,
            None,
            [],
            "s3://example_config_1",
            "example/config/1",
        ),
        # Provided all options, use config var
        (
            "example_config_1.json",
            "example_config_2.json",
            "example_config_3.json",
            None,
            [],
            "s3://example_config_1",
            "example/config/1",
        ),
        # Provided config var and current working directory, use config var
        (
            "example_config_1.json",
            None,
            "example_config_3.json",
            None,
            [],
            "s3://example_config_1",
            "example/config/1",
        ),
        # Provided env and current working directory, use env
        (
            None,
            "example_config_2.json",
            "example_config_3.json",
            None,
            [],
            "s3://example_config_2",
            "example/config/2",
        ),
        # Provided env, use env
        (
            None,
            "example_config_2.json",
            None,
            None,
            [],
            "s3://example_config_2",
            "example/config/2",
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
        ),
        # Missing bucket value from config
        (
            "example_config_4.json",
            None,
            None,
            None,
            [],
            constants.DEFAULT_QUILT_STORAGE,
            "example/config/4",
        ),
        # Missing local storage value from config
        (
            "example_config_5.json",
            None,
            None,
            None,
            [],
            "s3://example_config_5",
            file_utils.resolve_directory(
                constants.DEFAULT_LOCAL_TEMP_OUTPUTS.format(cwd=".", module_name="test")
            ),
        ),
        # Missing both values from config
        # Lol why use a config
        (
            "example_config_6.json",
            None,
            None,
            None,
            [],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_LOCAL_TEMP_OUTPUTS.format(cwd=".", module_name="test")
            ),
        ),
        # Checking upstream tasks
        (
            None,
            None,
            None,
            ["raw", "qc", "norm"],
            ["raw", "qc", "norm"],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_LOCAL_TEMP_OUTPUTS.format(cwd=".", module_name="test")
            ),
        ),
        # Checking when config doesn't exist
        pytest.param(
            "config_does_not_exist.json",
            None,
            None,
            None,
            [],
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
    expected_local_storage,
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
    t = Test(direct_upstream_tasks=direct_upstream_tasks, config=config_var)

    # Not required but may as well run it
    t.run()

    # Check init
    assert t._step_name == "test"
    assert t._upstream_tasks == expected_direct_upstream_tasks
    assert t._storage_bucket == expected_storage_bucket
    assert t._local_storage == expected_local_storage

    # Clear env
    os.environ.pop(constants.CONFIG_ENV_VAR_NAME, None)

    # Reset current working directory
    if use_cwd is not None:
        os.chdir(original_dir)
