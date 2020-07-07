#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This sample script will get deployed in the bin directory of the
user's virtualenv when the parent module is installed using pip.
"""

import argparse
import logging
import re
import sys
import traceback
from pathlib import Path

from jinja2 import Template

from datastep import exceptions, file_utils, get_module_version

###############################################################################

log = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)4s:%(lineno)4s %(asctime)s] %(message)s"
)

###############################################################################


class Args(argparse.Namespace):
    def __init__(self):
        # Arguments that could be passed in through the command line
        self.debug = False
        #
        self.__parse()

    def __parse(self):
        p = argparse.ArgumentParser(
            prog="make_new_step", description="Generate new step and directory."
        )
        p.add_argument(
            "-v",
            "--version",
            action="version",
            version="%(prog)s " + get_module_version(),
        )
        p.add_argument("step_name", help="The name of the step.")
        p.add_argument(
            "--debug", action="store_true", dest="debug", help=argparse.SUPPRESS
        )
        p.parse_args(namespace=self)


###############################################################################

# find where __all__ is set in the init file
def line_match__all__(py_txt):
    lines = [line for line in py_txt.split("\n") if "__all__" in line]
    assert len(lines) == 1
    return lines[0]


# find the string match for the list of classes that are set in __all__
def list_match_in_line(line, py_txt):
    class_list_str = re.findall(r"\[(.+?)\]", py_txt)
    assert len(class_list_str) == 1
    return class_list_str[0]


# insert the new class into the list set in __all__
def insert_new_class(old_list_string, new_class_name_string):
    new_list_string = f'{old_list_string}, "{new_class_name_string}"'
    return new_list_string


# find the last line of relative imports
def find_last_import_line(py_txt):
    lines = [line for line in py_txt.split("\n") if "from ." in line]
    return lines[-1]


# append our new import to the last one
def insert_new_import(last_old_line, new_class_name_string, new_class_dir_string):
    return (
        f"{last_old_line}\n"
        f"from .{new_class_dir_string} import {new_class_name_string}"
    )


###############################################################################


INIT_TEMPLATE = Template(
    """# -*- coding: utf-8 -*-

from .{{ step_name }} import {{ truecase_step_name }}  # noqa: F401

__all__ = ["{{ truecase_step_name }}"]

"""
)

STEP_TEMPLATE = Template(
    '''#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from datastep import Step, log_run_params

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class {{ truecase_step_name }}(Step):
    def __init__(
        self,
        direct_upstream_tasks: List["Step"] = [],
        config: Optional[Union[str, Path, Dict[str, str]]] = None,
    ):
        super().__init__(direct_upstream_tasks=direct_upstream_tasks, config=config)

    @log_run_params
    def run(self, **kwargs):
        """
        Run a pure function.

        Protected Parameters
        --------------------
        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
        clean: bool
            Should the local staging directory be cleaned prior to this run.
            Default: False (Do not clean)
        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc.
            Default: False (Do not debug)

        Parameters
        ----------

        Returns
        -------
        result: Any
            A pickable object or value that is the result of any processing you do.
        """
        # Your code here
        #
        # The `self.step_local_staging_dir` is exposed to save files in
        #
        # The user should set `self.manifest` to a dataframe of absolute paths that
        # point to the created files and each files metadata
        #
        # By default, `self.filepath_columns` is ["filepath"], but should be edited
        # if there are more than a single column of filepaths
        #
        # By default, `self.metadata_columns` is [], but should be edited to include
        # any columns that should be parsed for metadata and attached to objects
        #
        # The user should not rely on object state to retrieve results from prior steps.
        # I.E. do not call use the attribute self.upstream_tasks to retrieve data.
        # Pass the required path to a directory of files, the path to a prior manifest,
        # or in general, the exact parameters required for this function to run.
        return

'''
)


###############################################################################


def _find_steps_dir():
    filters = [".git", ".egg", "docs", "localstaging", ".ipynb"]

    for d in Path.cwd().iterdir():
        if d.is_dir():
            if not any(f in d.name for f in filters):
                for subd in d.iterdir():
                    if subd.name == "steps":
                        return subd

    return exceptions.DirectoryNotFoundError(
        "Could not find 'steps' directory."
        "This script must be run from the head of your repo."
    )


###############################################################################


def main():
    try:
        args = Args()
        dbg = args.debug

        # Assume the python module is the same name as the repo
        all_steps_dir = _find_steps_dir()

        # Normalize the provided name
        step_name = str(args.step_name).lower()
        truecase_step_name = "".join([token.title() for token in step_name.split("_")])

        # Make the directory
        this_step_dir = file_utils.resolve_directory(
            all_steps_dir / step_name, make=True
        )

        # Make the __init__ file
        with open(this_step_dir / "__init__.py", "w") as write_init:
            write_init.write(
                INIT_TEMPLATE.render(
                    step_name=step_name, truecase_step_name=truecase_step_name
                )
            )

        # Make the step file
        with open(this_step_dir / f"{step_name}.py", "w") as write_step_file:
            write_step_file.write(
                STEP_TEMPLATE.render(truecase_step_name=truecase_step_name)
            )

        # Mutate the all steps dir init file to include the new step
        all_steps_init = all_steps_dir / "__init__.py"

        # Read all steps init
        with open(all_steps_init, "r") as read_all_steps_init:
            current_all_steps_init_text = read_all_steps_init.read()

        # Format the __all__ modules list
        current_module_all_list_line = line_match__all__(current_all_steps_init_text)
        current_module_all_list = list_match_in_line(
            current_module_all_list_line, current_all_steps_init_text
        )
        new_module_all_list = insert_new_class(
            current_module_all_list, truecase_step_name
        )

        # Format the new last manual import
        current_last_import_line = find_last_import_line(current_all_steps_init_text)
        new_last_import_line = insert_new_import(
            current_last_import_line, truecase_step_name, step_name
        )

        # Replace old strings with new ones
        new_all_steps_init_text = current_all_steps_init_text.replace(
            current_module_all_list, new_module_all_list
        ).replace(current_last_import_line, new_last_import_line)

        # Write the new all steps init file
        with open(all_steps_init, "w") as write_all_steps_init:
            write_all_steps_init.write(new_all_steps_init_text)

        log.info(f"Generated new step file at: {this_step_dir}")

    except Exception as e:
        log.error("=============================================")
        if dbg:
            log.error("\n\n" + traceback.format_exc())
            log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")
        sys.exit(1)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
