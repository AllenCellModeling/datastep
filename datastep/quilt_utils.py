#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Dict, List, NamedTuple, Tuple, Union

import pandas as pd
from quilt3.packages import Package, PackageEntry
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
            details_type=details.details_type,
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
            details_type=details.details_type,
        )


def route_validator(
    details: ValidationDetails, manifest: pd.DataFrame, progress_bar
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

# VALIDATION


def validate_manifest(
    manifest: pd.DataFrame, filepath_columns: List[str], metadata_columns: List[str]
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
            details_to_validate_or_clean.append(
                ValidationDetails(
                    value=row[col], index=i, origin_column=col, details_type="path"
                )
            )

    # Create metadata cleaning details objects
    for col in metadata_columns:
        for i, row in manifest.iterrows():
            details_to_validate_or_clean.append(
                ValidationDetails(
                    value=row[col], index=i, origin_column=col, details_type="metadata"
                )
            )

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


###############################################################################

# PACKAGING


def _recursive_clean(pkg: Package, metadata_reduction_map: Dict[str, bool]):
    # For all keys in current package level
    for key in pkg:
        # If it is a PackageEntry object, we know we have hit a leaf node
        if isinstance(pkg[key], PackageEntry):
            # Reduce the metadata to a single value where it can
            cleaned_meta = {}
            for meta_k, meta_v in pkg[key].meta.items():
                # If the metadata reduction map at the metadata column (or meta_k) can
                # be reduced / collapsed (True), reduce /collapse the metadata
                # Reminder: this step will make the metadata access for every file of
                # the same file type the same format. Example: all files under the key
                # "FOV" will have the same metadata access after this function runs.
                # All the metadata access for the same file type across the package, if
                # one file has a list of values for the metadata key, "A", we want all
                # files of the same type to all have list of values for the metadata
                # key, "A". We also can't just use a set here for two reasons, the
                # first is simply that sets are not JSON serializable. "But you can
                # just cast to a set then back to a list!!!". The second reason is that
                # because a file can have multiple list of values in it's metadata, if
                # we cast to a set, one list may be reduced to two items while another,
                # different metadata list of values may be reduced to
                # a single item. Which leads to the problem of matching up metadata to
                # metadata for the same file.
                # The example to use here is looking at an FOV files metadata:
                # {"CellID": [1, 2, 3], "CellIndex": [4, 8, 12]}
                # By having them both as list without any chance of reduction means
                # that it is easy to match metadata values to each other.
                # "CellId" 1 maps to "CellIndex" 4, 2 maps to 8, and 3 maps to 12 in
                # this case.
                if metadata_reduction_map[meta_k]:
                    cleaned_meta[meta_k] = meta_v[0]
                # Else, do not reduce
                else:
                    cleaned_meta[meta_k] = meta_v

            # Update the object with the cleaned metadata
            pkg[key].set_meta(cleaned_meta)
        else:
            _recursive_clean(pkg[key], metadata_reduction_map)

    return pkg


def create_package(
    manifest: pd.DataFrame,
    step_pkg_root: Path,
    filepath_columns: List[str] = ["filepath"],
    metadata_columns: List[str] = [],
) -> Tuple[Package, pd.DataFrame]:
    # Make a copy
    relative_manifest = manifest.copy(deep=True)

    # Create empty package
    pkg = Package()

    # Create associate mappings: List[Dict[str, str]]
    # This list is in index order. Meaning that as the column values are descended we
    # can simply add a new associate to the already existing associate map at that list
    # index.
    associates = []

    # Create metadata reduction map
    # This will be used to clean up and standardize the metadata access after object
    # construction. Metadata column name to boolean value for should or should not
    # reduce metadata values. This will be used during the "clean up the package
    # metadata step". If we have multiple files each with the same keys for the
    # metadata, but for one reason or another, one packaged file's value for a certain
    # key is a list while another's is a single string, this leads to a confusing mixed
    # return value API for the same _type_ of object. Example:
    # fov/
    #   obj1/
    #      {example_key: "hello"}
    #   obj2/
    #      {example_key: ["hello", "world"]}
    # Commonly this happens when a manifest has rows of unique instances of a child
    # object but retains a reference to a parent object, example: rows of information
    # about unique cells that were all generated using the same algorithm, whose
    # information is stored in a column, for each cell information row. This could
    # result in some files (which only have one cell) being a single string while other
    # files (which have more than one cell) being a list of the same string over and
    # over again. "Why spend all this time to reduce/ collapse the metadata anyway?",
    # besides making it so that users won't have to call `obj2.meta["example_key"][0]`
    # every time they want the value, and besides the fact that it standardizes the
    # metadata api, the biggest reason is that S3 objects can only have 2KB of metadata,
    # without this reduction/ collapse step, manifests are more likely to hit that limit
    # and cause a package distribution error.
    metadata_reduction_map = {index_col: True for index_col in metadata_columns}

    # Set all files
    with tqdm(
        total=len(filepath_columns) * len(relative_manifest),
        desc="Constructing package",
    ) as pbar:
        for col in filepath_columns:
            # Update values to the logical key as they are set
            for i, val in enumerate(relative_manifest[col].values):
                # Fully resolve the path
                physical_key = Path(val).expanduser().resolve()

                # Try creating a logical key from the relative of step
                # local staging to the filepath
                #
                # Ex:
                # step_pkg_root = "local_staging/raw"
                # physical_key = "local_staging/raw/images/some_file.tiff"
                # produced logical_key = "images/some_file.tiff"
                try:
                    logical_key = str(
                        file_utils._filepath_rel2abs(physical_key).relative_to(
                            file_utils._filepath_rel2abs(step_pkg_root)
                        )
                    )

                except ValueError:
                    # Create logical key from merging column and filename
                    # Also remove any obvious "path" type words from column name
                    #
                    # Ex:
                    # physical_key = "/some/abs/path/some_file.tiff"
                    # column = "SourceReadPath"
                    # produced logical_key = "source/some_file.tiff"
                    stripped_col = col.lower().replace("read", "").replace("path", "")
                    logical_key = f"{stripped_col}/{physical_key.name}"

                if physical_key.is_file():
                    relative_manifest[col].values[i] = logical_key

                    # Create metadata dictionary to attach to object
                    meta = {}
                    for meta_col in metadata_columns:
                        # Short reference to current metadata value
                        v = relative_manifest[meta_col].values[i]

                        # Enforce simple JSON serializable type
                        # First check if value is a numpy value
                        # It likely is because pandas relies on numpy
                        # All numpy types have the "dtype" attribute and can be cast to
                        # python type by using the `item` function, details here:
                        # https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.item.html
                        if hasattr(v, "dtype"):
                            v = v.item()

                        # Cast to JSON serializable type
                        v = file_utils.make_json_serializable(
                            v, f"Value from column: {meta_col}, index: {i}"
                        )

                        # Update metadata with value
                        meta[meta_col] = [v]

                    # Check if object already exists
                    if logical_key in pkg:
                        # Join the two meta dictionaries
                        joined_meta = {}
                        for meta_col, curr_v in pkg[logical_key].meta.items():
                            # Join the values for the current iteration of the metadata
                            joined_values = [*curr_v, *meta[meta_col]]

                            # Only check if the metadata at this index can be reduced
                            # if currently is still being decided. We know if the
                            # metadata value at this index is still be decided if:
                            # the boolean value in the metadata reduction map is True,
                            # as in, this index can be reduced or collapsed.
                            # The other reason to make this check is so that we don't
                            # override an earlier False reduction value. In the case
                            # where early on we encounter an instance of the metadata
                            # that should not be reduced but then later on we say it
                            # can be, this check prevents that. As we want all metadata
                            # access across the dataset to be uniform.
                            if metadata_reduction_map[meta_col]:
                                # Update the metadata reduction map
                                # For the current column being checked, as long as it
                                # is still being determined that the column can be
                                # reduced (aka we have entered this if block) check if
                                # we can still reduce the metadata after the recent
                                # addition. "We can reduce the metadata if the count of
                                # the first value (or any value) is the same as the
                                # length of the entire list of values".
                                # This runs quickly for small lists as seen here:
                                # https://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
                                metadata_reduction_map[meta_col] = joined_values.count(
                                    joined_values[0]
                                ) == len(
                                    joined_values
                                )  # noqa F501

                            # Attached the joined values to the joined metadata
                            joined_meta[meta_col] = joined_values

                        # Update meta
                        pkg[logical_key].set_meta(joined_meta)

                    # Object didn't already exist, simply set it
                    else:
                        pkg.set(logical_key, physical_key, meta)

                    # Update associates
                    try:
                        associates[i][col] = logical_key
                    except IndexError:
                        associates.append({col: logical_key})
                else:
                    relative_manifest[col].values[i] = logical_key
                    pkg.set_dir(logical_key, physical_key)

                # Update progress bar
                pbar.update()

        # Clean up package metadata
        pkg = _recursive_clean(pkg, metadata_reduction_map)

        # Attach associates
        for i, associate_mapping in tqdm(
            enumerate(associates), desc="Creating associate metadata blocks"
        ):
            for col, lk in associate_mapping.items():
                # Having dictionary expansion in this order means that associates will
                # override a prior existing `associates` key, this is assumed safe
                # because attach_associates was set to True.
                pkg[lk].set_meta({**pkg[lk].meta, **{"associates": associate_mapping}})

        return pkg, relative_manifest
