"""
This module builds the request fragment
to be used in McM for a produced Gridpack.
Configuration is taken via GridpackFiles
repository.
"""

import os
from os.path import join as path_join
import json
import logging
from environment import GRIDPACK_FILES_PATH
from src.gridpack import Gridpack
from src.tools.utils import get_indentation


class FragmentBuilder:

    def __init__(self):
        self.logger = logging.getLogger()
        files_dir = GRIDPACK_FILES_PATH
        self.fragments_path = os.path.join(files_dir, "Fragments")
        self.imports_path = os.path.join(self.fragments_path, "imports.json")

    def build_fragment(self, gridpack):
        dataset_dict = gridpack.get_dataset_dict()
        file_list = dataset_dict.get("fragment", [])
        if isinstance(file_list, str):
            file_list = [file_list]

        self.logger.info("List of files for fragment builder: %s", ",".join(file_list))
        fragment = ""
        for file_name in file_list:
            with open(
                path_join(self.fragments_path, file_name), encoding="utf-8"
            ) as input_file:
                contents = input_file.read().strip()

            fragment += contents + "\n\n"

        fragment = self.fragment_replace(fragment, gridpack)
        return fragment

    def get_external_lhe_producer(self):
        path = os.path.join("Templates", "ExternalLHEProducer.dat")
        if not os.path.exists(path):
            raise Exception(f"Could not find {path} as external LHE producer")

        self.logger.debug("Reading %s", path)
        with open(path, encoding="utf-8") as input_file:
            contents = input_file.read()

        return f"{contents.strip()}\n"  # Add newline to the end of the contents

    def fragment_replace(self, fragment, gridpack: Gridpack):
        with open(self.imports_path, encoding="utf-8") as input_file:
            import_dict = json.load(input_file)

        dataset_dict = gridpack.get_dataset_dict()
        campaign_dict = gridpack.get_campaign_dict()
        fragment_vars = dataset_dict.get("fragment_vars", [])
        fragment_vars.update(campaign_dict.get("fragment_vars", {}))
        tune = gridpack.get("tune")
        beam = campaign_dict.get("beam", 0)
        fragment_vars["tuneName"] = tune
        fragment_vars["comEnergy"] = int(beam * 2)
        fragment_vars["tuneImport"] = import_dict["tune"][tune]

        # Set the final path
        final_archive_path = gridpack.get_absolute_path()

        # Set the Gridpack's path for the fragment
        # Replace the path if CMS GEN production folder is used,
        # its content is synchronized with /cvmfs.
        final_archive_path = final_archive_path.replace(
            "/eos/cms/store/group/phys_generator/cvmfs/gridpacks/",
            "/cvmfs/cms.cern.ch/phys_generator/gridpacks/",
        )
        fragment_vars["pathToProducedGridpack"] = final_archive_path
        for key, value in fragment_vars.items():
            if isinstance(value, list):
                indentation = " " * get_indentation(f"${key}", fragment)
                value = [f"{indentation}{x}" for x in value]
                value = ",\n".join(value).strip()

            fragment = fragment.replace(f"${key}", str(value))

        return fragment
