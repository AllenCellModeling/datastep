#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

###############################################################################


class Step(ABC):

    def __init__(self, direct_upstream_tasks: Optional[List[str]] = None):
        # Catch none
        if direct_upstream_tasks is None:
            self._upstream_tasks = []
        else:
            # TODO:
            # check that upstream tasks is a list of strings that match already
            # existing module names
            self._upstream_tasks = direct_upstream_tasks

    @abstractmethod
    def run(self, **kwargs):
        # Your code here
        pass

    @staticmethod
    def _pull_from_local(
        package_name: str,
        save_dir: Path,
        data_version: str,
        bucket: str = "/allen/aics/modeling/local-quilt"
    ):
        # Pull a package from local quilt
        pass

    @staticmethod
    def _pull_from_remote(
        package_name: str,
        save_dir: Path,
        data_version: str,
        bucket: str = "s3://allencell-internal-quilt"
    ):
        # Pull a package from remote quilt
        pass

    @staticmethod
    def _push_to_local(
        package_name: str,
        files: Union[str, Path, List[str, Path], pd.DataFrame],
        bucket: str = "/allen/aics/modeling/local-quilt"
    ):
        # Push a package to local quilt
        pass

    @staticmethod
    def _push_to_remote(
        package_name: str,
        files: Union[str, Path, List[str, Path], pd.DataFrame],
        bucket: str = "s3://allencell-internal-quilt"
    ):
        # Push a package to remote quilt
        pass

    def pull(
        self,
        save_dir: Union[str, Path] = "",
        local: bool = False,
        data_version: Optional[str] = None
    ):
        # Route to correct pull function
        if local:
            pull_func = Step._pull_from_local
        else:
            pull_func = Step._pull_from_remote

        # Convert save_dir to Path and resolve

        # Run pull for each upstream
        for upstream_task in self._upstream_tasks:
            pull_func(
                # package_name=f"{__PACKAGE_NAME__}{__MODULE_NAME__}"
                # save_dir=Path()
            )

    def checkout(
        self,
        save_dir: Union[str, Path] = "",
        local: bool = False,
        data_version: Optional[str] = None
    ):
        # Get module name and pull
        pass

    def push(
        self,
        files: Union[str, Path, List[str, Path], pd.DataFrame] = "",
        local: bool = False
    ):
        # Get module name and push
        pass
