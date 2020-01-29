#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

###############################################################################

log = logging.getLogger(__name__)

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
    return filepath.resolve().relative_to(otherpath.resolve())


def manifest_filepaths_rel2abs(mystep):
    for col in mystep.filepath_columns:
        mystep.manifest[col] = mystep.manifest[col].apply(
            lambda x: str(
                _filepath_rel2abs(Path(x), prefixpath=mystep.step_local_staging_dir)
            )
        )


def manifest_filepaths_abs2rel(mystep):
    for col in mystep.filepath_columns:
        mystep.manifest[col] = mystep.manifest[col].apply(
            lambda x: str(_filepath_abs2rel(Path(x), mystep.step_local_staging_dir))
        )
