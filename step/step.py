#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Union

from . import file_utils

###############################################################################


class Step(ABC):

    def _unpack_config(
        self,
        config: Optional[Union[str, Path, Dict[str, str]]] = None
    ):
        # Easy case is a path to config was provided
        if config is not None:
            pass

        # Check environment
        elif "STEP_CONFIG" in os.environ:
            config = os.environ["STEP_CONFIG"]

        else:
            # Check current working directory
            cwd = Path().resolve()
            cwd_files = [str(f.name) for f in cwd.iterdir()]

            if "step_config.json" in cwd_files:
                config = cwd / "step_config.json"
            else:
                raise FileNotFoundError()

        # Check path like
        if isinstance(config, (str, Path)):
            # Resolve path
            config = file_utils.resolve_filepath(config)

            # Read
            with open(config, "r") as read_in:
                config = json.load(read_in)

        # Config should now either have been provided as a dict or parsed
        if isinstance(config, dict):
            self._storage_bucket = config.get(
                "quilt_storage_bucket",
                "s3://allencell-internal-quilt"
            )
            self._local_storage = config.get(
                "local_temp_outputs",
                file_utils.resolve_directory(
                    Path().resolve() / "temp" / f"{__name__}",
                    make=True
                )
            )

        # Config wasn't a valid type
        else:
            raise TypeError(
                f"Step config should be JSON formated file or Dictionary. "
                f"Received: {type(config)}: {str(config)}"
            )

    def __init__(self, direct_upstream_tasks: Optional[List[str]] = None):
        # Catch none
        if direct_upstream_tasks is None:
            self._upstream_tasks = []
        else:
            self._upstream_tasks = direct_upstream_tasks

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
        bucket: Optional[str] = None
    ):
        # Resolve None bucket
        if bucket is None:
            bucket = self._storage_bucket

        # Resolve save_dir
        save_dir = file_utils.resolve_directory(save_dir, make=True)

        pass

    def push(
        self,
        push_dir: Union[str, Path] = "",
        bucket: Optional[str] = None,
    ):
        # Resolve None bucket
        if bucket is None:
            bucket = self._storage_bucket

        # Resolve push_dir
        push_dir = file_utils.resolve_directory(push_dir)

        # Get module name and push
        pass
