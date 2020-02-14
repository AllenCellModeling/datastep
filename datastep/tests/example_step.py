#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import pandas as pd

from datastep import Step

#######################################################################################


# A dummy class to test with
class ExampleStep(Step):
    def run(self, N=3):
        # make a directory of empty files
        imdir = self.step_local_staging_dir / Path("files")
        imdir.mkdir(parents=True, exist_ok=True)

        # make a dataframe logging their absolute paths
        self.manifest = pd.DataFrame(index=range(N), columns=["filepath"])
        for i in range(N):
            path = imdir / Path(f"file{i}.txt")
            path = path.resolve()
            path.touch()
            self.manifest.at[i, "filepath"] = path

        # save manifest
        self.manifest.to_csv(
            self.step_local_staging_dir / Path("manifest.csv"), index=False
        )
