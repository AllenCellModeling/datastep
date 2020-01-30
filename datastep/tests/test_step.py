#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from pathlib import Path

import pytest
import pandas as pd

from datastep import Step, constants, file_utils

###############################################################################


# A dummy class to test with
class Test(Step):
    def __init__(self, direct_upstream_tasks=None, config=None):
        super().__init__(direct_upstream_tasks, config)

    def run(self, N=3):
        # make a directory of empty files
        imdir = self.step_local_staging_dir / Path("files")
        imdir.mkdir(parents=True, exist_ok=True)

        # make a dataframe logging their absolute paths
        self.manifest = pd.DataFrame(index=range(N), columns=["filepath"])
        for i in range(N):
            path = imdir / Path(f"file{i}.txt")
            path = path.resolve()
            path.touch()
            self.manifest.at[i, "filepath"] = path

        # save manifest
        self.manifest.to_csv(
            self.step_local_staging_dir / Path("manifest.csv"), index=False
        )


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
            None,
            [],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="test"
                ),
                make=True,
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
            "example/config/1/test",
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
            "example/config/1/test",
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
            "example/config/1/test",
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
            "example/config/1/test",
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
            "example/config/2/test",
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
            "example/config/2/test",
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
            "example/config/3/test",
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
            "example/config/4/test",
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
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="test"
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
            None,
            [],
            constants.DEFAULT_QUILT_STORAGE,
            file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="test"
                ),
                make=True,
            ),
        ),
        # Specific output directory for step available
        (
            "example_config_7.json",
            None,
            None,
            None,
            [],
            "s3://example_config_7",
            "example/config/7",
            "example/step/local/staging/7",
        ),
        # Step has key in config but no specific output directory listed
        (
            "example_config_8.json",
            None,
            None,
            None,
            [],
            "s3://example_config_8",
            "example/config/8",
            "example/config/8/test",
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
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True
            ),
            file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name="test"
                ),
                make=True,
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
    t = Test(direct_upstream_tasks=direct_upstream_tasks, config=config_var)

    # Not required but may as well run it
    t.run()

    # Check init
    assert t.step_name == "test"
    assert t.upstream_tasks == expected_direct_upstream_tasks
    assert t.storage_bucket == expected_storage_bucket
    assert str(expected_project_local_staging_dir) in str(t.project_local_staging_dir)
    assert str(expected_step_local_staging_dir) in str(t.step_local_staging_dir)

    # Clear env
    os.environ.pop(constants.CONFIG_ENV_VAR_NAME, None)

    # Reset current working directory
    if use_cwd is not None:
        os.chdir(original_dir)
