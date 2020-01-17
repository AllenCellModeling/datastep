#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Union

from . import constants, file_utils

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


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
                    f"{self.project_local_staging_dir / self.step_name}", make=True,
                )

        else:
            log.debug(f"Using default project and step configuration.")
            self._storage_bucket = constants.DEFAULT_QUILT_STORAGE
            self._project_local_staging_dir = file_utils.resolve_directory(
                constants.DEFAULT_PROJECT_LOCAL_STAGING_DIR.format(cwd="."), make=True,
            )
            self._step_local_staging_dir = file_utils.resolve_directory(
                constants.DEFAULT_STEP_LOCAL_STAGING_DIR.format(
                    cwd=".", module_name=self.step_name
                ),
                make=True,
            )

    def __init__(
        self,
        direct_upstream_tasks: Optional[List[str]] = None,
        config: Optional[Union[str, Path, Dict[str, str]]] = None,
    ):
        # Set step name
        self._step_name = self.__class__.__name__.lower()

        # Catch none
        if direct_upstream_tasks is None:
            self._upstream_tasks = []
        else:
            self._upstream_tasks = direct_upstream_tasks

        # Unpack config
        self._unpack_config(config)

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

    @abstractmethod
    def _run(self, **kwargs):
        # Your code here
        pass

    def run(self, **kwargs):
        # Pass through run
        self._run(**kwargs)

        # Get and clean parameters that ran
        params = locals()
        params.pop("self")

        # Store parameters
        parameter_store = self.step_local_staging_dir / "parameters.json"
        with open(parameter_store, "w") as write_out:
            json.dump(params, write_out)
            log.debug(f"Stored params for run at: {parameter_store}")

    def pull(
        self,
        save_dir: Union[str, Path] = "",
        data_version: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        # Resolve None bucket
        if bucket is None:
            bucket = self._storage_bucket

        # Resolve save_dir
        save_dir = file_utils.resolve_directory(save_dir, make=True)

        # Run pull for each upstream
        for upstream_task in self.upstream_tasks:
            pass

    def checkout(
        self,
        save_dir: Union[str, Path] = "",
        data_version: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        # Resolve None bucket
        if bucket is None:
            bucket = self._storage_bucket

        # Resolve save_dir
        save_dir = file_utils.resolve_directory(save_dir, make=True)

        pass

    def push(
        self, push_dir: Union[str, Path] = "", bucket: Optional[str] = None,
    ):
        # Resolve None bucket
        if bucket is None:
            bucket = self._storage_bucket

        # Resolve push_dir
        push_dir = file_utils.resolve_directory(push_dir)

        # Get module name and push
        pass

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
