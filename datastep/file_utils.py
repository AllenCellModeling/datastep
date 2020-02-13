#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import json
import logging
from pathlib import Path
from shutil import rmtree
from typing import Any, Dict, List, Optional, Union

import pandas as pd

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


def resolve_filepath(f: Union[str, Path], strict: bool = True) -> Path:
    # Resolve
    f = Path(f).expanduser()

    # Check is not dir
    if f.is_dir():
        raise IsADirectoryError(f)

    # Check if we want to fully resolve
    if strict:
        f = f.resolve(strict=True)
    else:
        if not f.exists():
            raise FileNotFoundError(f)

    return f


def resolve_directory(
    d: Union[str, Path], make: bool = False, strict: bool = True
) -> Path:
    # Expand user
    d = Path(d).expanduser()

    # Check if file
    if d.is_file():
        raise FileExistsError(d)

    # Make the directory if specified
    if make:
        d.mkdir(parents=True, exist_ok=True)

    # Check if we want to fully resolve
    if strict:
        # Fully resolve
        d = d.resolve(strict=True)
    else:
        if not d.exists():
            raise FileNotFoundError(d)

    return d


def create_unique_logical_key(physical_key: Union[str, Path]) -> str:
    # Fully resolve the phyiscal key
    pk = Path(physical_key).expanduser().resolve(strict=True)

    # Creat short hash from fully resolved physical key
    short_hash = hashlib.sha256(str(pk).encode("utf-8")).hexdigest()[:8]

    # Return the unique logical key
    return f"{short_hash}_{pk.name}"


def make_json_serializable(
    value: Any, context: Optional[str] = None
) -> Union[bool, float, int, str, List, Dict]:
    # Try dumping to JSON string
    try:
        json.dumps(value)
        return value
    # It isn't explicitly JSON serializable, convert to string
    except TypeError:
        if context is None:
            context = ""
        log.debug(f"Casting {value} to string to make JSON serializable. {context}")
        return str(value)


def _filepath_rel2abs(filepath: Path, prefixpath: Path = Path(".")) -> Path:
    return (prefixpath / filepath).resolve()


def _filepath_abs2rel(filepath: Path, otherpath: Path = Path(".")) -> Path:
    return (otherpath / filepath).resolve().relative_to(otherpath.resolve())


def manifest_filepaths_rel2abs(
    manifest: pd.DataFrame, filepath_columns: List[str], relative_dir: Path
):
    # Make a copy of the manifest
    manifest = manifest.copy(deep=True)

    # Run for each column in filepath columns
    for col in filepath_columns:
        manifest[col] = manifest[col].apply(
            lambda x: str(_filepath_rel2abs(Path(x), relative_dir))
        )

    return manifest


def manifest_filepaths_abs2rel(
    manifest: pd.DataFrame, filepath_columns: List[str], relative_dir: Path
):
    # Make a copy of the manifest
    manifest = manifest.copy(deep=True)

    # Run for each column in filepath columns
    for col in filepath_columns:
        manifest[col] = manifest[col].apply(
            lambda x: str(_filepath_abs2rel(Path(x), relative_dir))
        )

    return manifest


def _clean(dirpath: Path) -> Optional[Exception]:
    # Remove anything in step staging dir
    rmtree(dirpath)

    # Create it again as empty dir
    dirpath.mkdir(parents=True, exist_ok=True)


def _sanitize_name(input_str: str):
    return input_str.replace(" ", "_")
