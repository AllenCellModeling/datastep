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
        # Handle config provided as variable
        if config is not None:
            pass

        # Check other places config info could live
        else:
            # Check environment
            if constants.CONFIG_ENV_VAR_NAME in os.environ:
                config = os.environ[constants.CONFIG_ENV_VAR_NAME]

            else:
                # Check current working directory
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
            self._local_storage = file_utils.resolve_directory(
                config.get(
                    "local_temp_outputs",
                    constants.DEFAULT_LOCAL_TEMP_OUTPUTS.format(
                        cwd=".", module_name=self._step_name
                    )
                ),
                make=True,
            )

        else:
            log.debug(f"Using default configuration.")
            self._storage_bucket = constants.DEFAULT_QUILT_STORAGE
            self._local_storage = file_utils.resolve_directory(
                constants.DEFAULT_LOCAL_TEMP_OUTPUTS.format(
                    cwd=".", module_name=self._step_name
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

    @abstractmethod
    def run(self, **kwargs):
        # Your code here
        pass

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
        for upstream_task in self._upstream_tasks:
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
            f"<{self.__class__.__name__} "
            f"[local_temp_outputs: '{self._local_storage}', "
            f"storage_bucket: '{self._storage_bucket}']>"
        )

    def __repr__(self):
        return str(self)
