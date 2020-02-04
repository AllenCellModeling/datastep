#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import json
import logging
import os
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional, Union

import git
import pandas as pd
import quilt3

from . import constants, exceptions, file_utils, quilt_utils

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


# decorator for run that logs non default args and kwargs to file
def log_run_params(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        params = inspect.signature(func).bind(self, *args, **kwargs).arguments
        params.pop("self")
        parameter_store = self.step_local_staging_dir / "run_parameters.json"
        with open(parameter_store, "w") as write_out:
            json.dump(params, write_out, default=str)
            log.debug(f"Stored params for run at: {parameter_store}")
        return func(self, *args, **kwargs)

    return wrapper


class Step(ABC):
    def _unpack_config(self, config: Optional[Union[str, Path, Dict[str, str]]] = None):
        # If not provided, check for other places the config could live
        if config is None:
            # Check environment
            if constants.CONFIG_ENV_VAR_NAME in os.environ:
                config = os.environ[constants.CONFIG_ENV_VAR_NAME]

            # Check current working directory
            else:
                cwd = Path().resolve()
                cwd_files = [str(f.name) for f in cwd.iterdir()]

                # Attach config file name to cwd path
                if constants.CWD_CONFIG_FILE_NAME in cwd_files:
                    config = cwd / constants.CWD_CONFIG_FILE_NAME

        # Config should now either be path to JSON, Dict, or None
        if isinstance(config, (str, Path)):
            # Resolve path
            config = file_utils.resolve_filepath(config)

            # Read config
            with open(config, "r") as read_in:
                config = json.load(read_in)

        # Config should now either have been provided as a dict, parsed, or None
        if isinstance(config, dict):
            # Get or default
            self._storage_bucket = config.get(
                "quilt_storage_bucket", constants.DEFAULT_QUILT_STORAGE
            )
            # Get or default
            self._quilt_package_owner = config.get(
                "quilt_package_owner", constants.DEFAULT_QUILT_PACKAGE_OWNER
            )

            # Get or default project local staging
            self._project_local_staging_dir = file_utils.resolve_directory(
                config.get(
                    "project_local_staging_dir",
                    constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."),
                ),
                make=True,
            )

            # Get or default step local staging
            if self.step_name in config:
                self._step_local_staging_dir = file_utils.resolve_directory(
                    config[self.step_name].get(
                        "step_local_staging_dir",
                        f"{self.project_local_staging_dir / self.step_name}",
                    ),
                    make=True,
                )
            else:
                self._step_local_staging_dir = file_utils.resolve_directory(
                    f"{self.project_local_staging_dir / self.step_name}", make=True
                )

        else:
            config = {
                "quilt_storage_bucket": constants.DEFAULT_QUILT_STORAGE,
                "quilt_package_owner": constants.DEFAULT_QUILT_PACKAGE_OWNER,
                "project_local_staging_dir": file_utils.resolve_directory(
                    constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."),
                    make=True,
                ),
                self.step_name: {
                    "step_local_staging_dir": file_utils.resolve_directory(
                        constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                            cwd=".", module_name=self.step_name
                        ),
                        make=True,
                    )
                },
            }
            log.debug(f"Using default project and step configuration.")
            self._storage_bucket = config["quilt_storage_bucket"]
            self._quilt_package_owner = config["quilt_package_owner"]
            self._project_local_staging_dir = config["project_local_staging_dir"]
            self._step_local_staging_dir = config[self.step_name][
                "step_local_staging_dir"
            ]

            return config

    def __init__(
        self,
        clean_before_run=True,
        filepath_columns=["filepath"],
        metadata_columns=[],
        step_name=None,
        package_name=None,
        direct_upstream_tasks: List["Step"] = [],
        config: Optional[Union[str, Path, Dict[str, str]]] = None,
    ):

        # grab args/kwarg to write out at end of init
        params = locals()
        params.pop("self")

        # Set names as attributes if not None
        self._step_name = (
            file_utils._sanitize_name(step_name)
            if step_name is not None
            else self.__class__.__name__.lower()
        )
        self._package_name = (
            file_utils._sanitize_name(package_name)
            if package_name is not None
            else self.__module__.split(".")[0]
        )

        # Set kwargs as attributes
        self._upstream_tasks = direct_upstream_tasks
        self.filepath_columns = filepath_columns
        self.metadata_columns = metadata_columns

        # Unpack config into param log dict
        params["config"] = self._unpack_config(config)

        # clean old files
        if clean_before_run:
            file_utils._clean(self.step_local_staging_dir)

        # write out args/kwargs now that we know where
        parameter_store = self.step_local_staging_dir / "init_parameters.json"
        with open(parameter_store, "w") as write_out:
            json.dump(params, write_out, default=str)
            log.debug(f"Stored params for run at: {parameter_store}")

        # Set defaults -- TODO log manifest find or not
        m_path = Path(self.step_local_staging_dir / "manifest.csv")
        self.manifest = pd.read_csv(m_path) if m_path.is_file() else None

    @property
    def step_name(self) -> str:
        return self._step_name

    @property
    def upstream_tasks(self) -> List[str]:
        return self._upstream_tasks

    @property
    def storage_bucket(self) -> str:
        return self._storage_bucket

    @property
    def project_local_staging_dir(self) -> Path:
        return self._project_local_staging_dir

    @property
    def step_local_staging_dir(self) -> Path:
        return self._step_local_staging_dir

    @property
    def package_name(self) -> str:
        return self._package_name

    @property
    def quilt_package_owner(self) -> str:
        return self._quilt_package_owner

    @abstractmethod
    def run(self, **kwargs):
        # Your code here
        #
        # The `self.step_local_staging_dir` is exposed to save files in
        #
        # The user should set `self.manifest` to a dataframe of absolute paths that
        # point to the created files and each files metadata
        #
        # By default, `self.filepath_columns` is ["filepath"], but should be edited
        # if there are more than a single column of filepaths
        #
        # By default, `self.metadata_columns` is [], but should be edited to include
        # any columns that should be parsed for metadata and attached to objects
        pass

    def pull(self, data_version: Optional[str] = None, bucket: Optional[str] = None):
        # Resolve None bucket
        if bucket is None:
            bucket = self.storage_bucket

        # Run checkout for each upstream
        for UpstreamTask in self.upstream_tasks:
            upstream_task = UpstreamTask()
            upstream_task.checkout(data_version=data_version, bucket=bucket)

    @staticmethod
    def _get_current_git_branch() -> str:
        repo = git.Repo(Path(".").expanduser().resolve())
        return repo.active_branch.name

    @staticmethod
    def _check_git_status_is_clean(push_target: str) -> Optional[Exception]:
        # This will throw an error if the current working directory is not a git repo
        repo = git.Repo(Path(".").expanduser().resolve())
        current_branch = repo.active_branch.name

        # Check current git status
        if repo.is_dirty() or len(repo.untracked_files) > 0:
            dirty_files = [f.b_path for f in repo.index.diff(None)]
            all_changed_files = repo.untracked_files + dirty_files
            raise exceptions.InvalidGitStatus(
                f"Push to '{push_target}' was rejected because the current git "
                f"status of this branch ({current_branch}) is not clean. "
                f"Check files: {all_changed_files}."
            )

    @staticmethod
    def _create_data_commit_message() -> str:
        # This will throw an error if the current working directory is not a git repo
        repo = git.Repo(Path(".").expanduser().resolve())
        current_branch = repo.active_branch.name

        return (
            f"data created from code repo {repo.remotes.origin.url} on branch "
            f"{current_branch} at commit {repo.head.object.hexsha}"
        )

    def checkout(
        self, data_version: Optional[str] = None, bucket: Optional[str] = None
    ):
        # Resolve None bucket
        if bucket is None:
            bucket = self.storage_bucket

        # Get current git branch
        current_branch = self._get_current_git_branch()

        # Checkout this step's output from quilt
        # Check for files on this branch and default to master

        # Browse top level project package
        quilt_loc = f"{self.quilt_package_owner}/{self.package_name}"
        p = quilt3.Package.browse(quilt_loc, bucket, top_hash=data_version)

        # Check to see if step data exists on this branch in quilt
        try:
            quilt_branch_step = f"{current_branch}/{self.step_name}"
            p[quilt_branch_step]

        # If not, use the version on master
        except KeyError:
            quilt_branch_step = f"master/{self.step_name}"
            p[quilt_branch_step]

        # Fetch the data and save it to the local staging dir
        p[quilt_branch_step].fetch(self.step_local_staging_dir)

    def push(self, bucket: Optional[str] = None):
        # Resolve None bucket
        if bucket is None:
            bucket = self.storage_bucket

        # Get current git branch
        current_branch = self._get_current_git_branch()

        # Resolve push target
        quilt_loc = f"{self.quilt_package_owner}/{self.package_name}"
        push_target = f"{quilt_loc}/{current_branch}/{self.step_name}"

        # Check git status is clean
        self._check_git_status_is_clean(push_target)

        # Construct the package
        step_pkg = quilt_utils.create_package(
            manifest=self.manifest,
            step_pkg_root=self.step_local_staging_dir,
            filepath_columns=self.filepath_columns,
            metadata_columns=self.metadata_columns,
        )

        # Add the manifest to the package
        # TODO error on None manifest
        m_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(m_path, index=False)
        step_pkg.set("manifest.csv", m_path)

        # Add the params files to the package
        for param_file in ["run_parameters.json", "init_parameters.json"]:
            param_file_path = self.step_local_staging_dir / param_file
            step_pkg.set(param_file, param_file_path)

        # Browse top level project package and add / overwrite to it in step dir
        project_pkg = quilt3.Package.browse(quilt_loc, self.storage_bucket)
        for (logical_key, pkg_entry) in step_pkg.walk():
            project_pkg.set(
                f"{current_branch}/{self.step_name}/{logical_key}", pkg_entry
            )

        # Push the data
        project_pkg.push(
            quilt_loc,
            registry=self.storage_bucket,
            message=self._create_data_commit_message(),
        )

    def clean(self) -> str:
        file_utils._clean(self.step_local_staging_dir)

    def __str__(self):
        return (
            f"<{self.step_name} [ "
            f"upstream_tasks: {self.upstream_tasks}, "
            f"storage_bucket: '{self.storage_bucket}', "
            f"project_local_staging_dir: '{self.project_local_staging_dir}', "
            f"step_local_staging_dir: '{self.step_local_staging_dir}' "
            f"]>"
        )

    def __repr__(self):
        return str(self)
