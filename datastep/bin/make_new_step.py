#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This sample script will get deployed in the bin directory of the
users' virtualenv when the parent module is installed using pip.
"""

import argparse
import logging
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

INIT_TEMPLATE = Template(
    """# -*- coding: utf-8 -*-

from .{{ step_name }} import {{ truecase_step_name }}  # noqa: F401

"""
)

STEP_TEMPLATE = Template(
    """#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from datastep import Step, log_params

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class {{ truecase_step_name }}(Step):
    def __init__(
        self,
        direct_upstream_tasks: Optional[List["Step"]] = None,
        config: Optional[Union[str, Path, Dict[str, str]]] = None,
    ):
        super().__init__(direct_upstream_tasks, config)

    @log_run_params
    def run(self, **kwargs):
        # Your code here
        #
        # The `self.step_local_staging_dir` is exposed for you to save files in
        #
        # You should set `self.manifest` to a dataframe of relative paths that
        # point to the created files and each file's metadata
        #
        # By default, `self.filepath_columns` is ["filepath"], but should be edited
        # if there are more than a single column of filepaths in your manifest.
        #
        # By default, `self.metadata_columns` is [], but should be edited to include
        # any columns that should be parsed for metadata and attached to objects

        pass

"""
)


###############################################################################


def main():
    try:
        args = Args()
        dbg = args.debug

        # Check if the step already exists
        cwd = Path(".").expanduser().resolve()

        # Find steps subdirectory
        steps_dir_finds = list(cwd.rglob("steps"))

        # Catch no steps dir found
        if len(steps_dir_finds) == 0:
            raise exceptions.DirectoryNotFoundError(
                "Could not find any subdirectory named 'steps' in the current working "
                "directory."
            )
        # Catch multiple found
        elif len(steps_dir_finds) > 1:
            raise exceptions.DirectoryNotFoundError(
                f"Found multiple subdirectories named 'steps' in the current working "
                f"directory. Found: {steps_dir_finds}"
            )

        # Properly found
        all_steps_dir = steps_dir_finds[0]

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
