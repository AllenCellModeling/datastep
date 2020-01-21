#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Dict, List, NamedTuple, Union

import pandas as pd
from quilt3 import Package
from tqdm import tqdm

from . import file_utils

###############################################################################


class ValidationDetails(NamedTuple):
    value: Union[int, str, float, bool, Dict, List, Path]
    index: int
    origin_column: str
    details_type: str


def validate_filepath(details: ValidationDetails) -> ValidationDetails:
    try:
        return ValidationDetails(
            value=file_utils.resolve_filepath(details.value),
            index=details.index,
            origin_column=details.origin_column,
            details_type=details.details_type
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Failed to find file: '{details.value}'. "
            f"Source column: '{details.origin_column}', "
            f"at index: {details.index}."
        )
    except IsADirectoryError:
        raise IsADirectoryError(
            f"Paths to directories are not allowed. Please be explicit in which files "
            f"should be uploaded. Directory: '{details.value}', "
            f"source column: '{details.origin_column}', "
            f"at index: {details.index}."
        )


def clean_metadata(details: ValidationDetails) -> ValidationDetails:
    try:
        json.dumps(details.value)
        return details
    except TypeError:
        return ValidationDetails(
            value=str(details.value),
            index=details.index,
            origin_column=details.origin_column,
            details_type=details.details_type
        )


def route_validator(
    details: ValidationDetails,
    manifest: pd.DataFrame,
    progress_bar
) -> ValidationDetails:
    if details.details_type == "path":
        result = validate_filepath(details)
    else:
        result = clean_metadata(details)

    # Update value with validated
    manifest.at[result.index, result.origin_column] = result.value

    # Update progress
    progress_bar.update()

    return result


###############################################################################


def validate_manifest(
    manifest: pd.DataFrame,
    filepath_columns: List[str],
    metadata_columns: List[str]
):
    # Check filepath columns exist in manifest
    for col in filepath_columns:
        if col not in manifest.columns:
            raise ValueError(
                f"Could not find filepath column: '{col}' "
                f"in manifest columns: {manifest.columns}"
            )

    # Check filepath columns exist in manifest
    for col in metadata_columns:
        if col not in manifest.columns:
            raise ValueError(
                f"Could not find metadata column: '{col}' "
                f"in manifest columns: {manifest.columns}"
            )

    # Create large list of paths to validate and metadata to clean
    details_to_validate_or_clean = []

    # Create filepath validation details objects
    for col in filepath_columns:
        for i, row in manifest.iterrows():
            details_to_validate_or_clean.append(ValidationDetails(
                value=row[col],
                index=i,
                origin_column=col,
                details_type="path"
            ))

    # Create metadata cleaning details objects
    for col in metadata_columns:
        for i, row in manifest.iterrows():
            details_to_validate_or_clean.append(ValidationDetails(
                value=row[col],
                index=i,
                origin_column=col,
                details_type="metadata"
            ))

    # Create progress bar
    with tqdm(total=len(details_to_validate_or_clean), desc="Validating") as pbar:
        # Create a deep copy of the dataframe to mutate
        manifest = manifest.copy(deep=True)

        # Create validator partial
        validator_func = partial(route_validator, manifest=manifest, progress_bar=pbar)

        # Threaded validation and update
        with ThreadPoolExecutor() as exe:
            # We cast to a list to force a block until all are done
            list(exe.map(validator_func, details_to_validate_or_clean))

    return manifest
