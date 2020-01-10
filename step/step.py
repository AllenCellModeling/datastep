#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Union

###############################################################################


class Step(ABC):

    def __init__(self, direct_upstream_tasks: Optional[List[str]] = None):
        self._upstream_tasks = direct_upstream_tasks

    @abstractmethod
    def run(self, **kwargs):
        # Your code here
        pass

    def pull(
        self,
        save_dir: Union[str, Path] = "",
        local: bool = False,
        data_version: Optional[str] = None
    ):
        # Get upstream tasks and pull them
        pass

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
        files: Union[str, Path] = "",
        local: bool = False
    ):
        # Get module name and push
        pass
