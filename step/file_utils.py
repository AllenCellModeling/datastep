#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from typing import Union

###############################################################################


def resolve_filepath(f: Union[str, Path]) -> Path:
    # Resolve
    f = Path(f).expanduser().resolve(strict=True)

    # Check is not dir
    if f.is_dir():
        raise IsADirectoryError(f)

    return f


def resolve_directory(d: Union[str, Path], make: bool = False) -> Path:
    # Expand user
    d = Path(d).expanduser()

    # Check if file
    if d.is_file():
        raise FileExistsError(d)

    # Make the directory if specified
    if make:
        d.mkdir(parents=True, exist_ok=True)

    # Fully resolve
    d = d.resolve(strict=True)

    return d
