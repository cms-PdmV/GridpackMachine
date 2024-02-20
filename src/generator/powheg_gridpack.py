"""
Implementation for Gridpacks
related with Powheg generator.
"""

import os
import pathlib
from src.gridpack import Gridpack


class PowhegGridpack(Gridpack):

    def get_run_card(self):
        """
        Get cards from "Template" directory and customize them
        Get cards from "ModelParams" directory and customize them
        Glue them together
        """
        dataset_dict = self.get_dataset_dict()
        campaign_dict = self.get_campaign_dict()
        templates_path = self.get_templates_path()
        template_name = dataset_dict["template"]
        template_vars = dataset_dict.get("template_vars", [])
        template_vars["ebeam1"] = campaign_dict.get("beam", 0)
        template_vars["ebeam2"] = template_vars["ebeam1"]
        template_vars.update(campaign_dict.get("template_vars", {}))
        input_file_name = os.path.join(templates_path, template_name)
        run_card = self.customize_file(
            input_file_name, dataset_dict.get("template_user", []), template_vars
        )

        model_params_path = self.get_model_params_path()
        model_params_name = dataset_dict["model_params"]
        model_params_vars = dataset_dict.get("model_params_vars", [])
        model_params_vars.update(campaign_dict.get("model_params_vars", {}))
        input_file_name = os.path.join(model_params_path, model_params_name)
        customize_card = self.customize_file(
            input_file_name,
            dataset_dict.get("model_params_user", []),
            model_params_vars,
        )

        return run_card + "\n" + customize_card

    def prepare_run_card(self):
        """
        Get run card and write it to job files dir
        """
        job_files_path = self.get_job_files_path()
        output_file_name = os.path.join(job_files_path, "powheg.input")
        run_card = self.get_run_card()
        self.logger.debug("Writing customized run card %s", output_file_name)
        self.logger.debug(run_card)
        with open(output_file_name, "w", encoding="utf-8") as output_file:
            output_file.write(run_card)

    def get_customize_card(self):
        """
        Create card with just the process name for proper Powheg gridpack
        production
        """
        dataset_dict = self.get_dataset_dict()
        template_name = dataset_dict["template"]
        return template_name.split(".", 1)[0]

    def prepare_customize_card(self):
        """
        Get customize card and write it to job files dir
        """
        job_files_path = self.get_job_files_path()
        output_file_name = os.path.join(job_files_path, "process.dat")
        customize_card = self.get_customize_card()
        self.logger.debug("Writing customized card %s", output_file_name)
        self.logger.debug(customize_card)
        with open(output_file_name, "w", encoding="utf-8") as output_file:
            output_file.write(customize_card)

    def prepare_job_archive(self):
        """
        Make an archive with all necessary job files
        """
        job_files_path = self.get_job_files_path()
        pathlib.Path(job_files_path).mkdir(parents=True, exist_ok=True)
        self.prepare_run_card()
        self.prepare_customize_card()
        local_dir = self.local_dir()
        os.system(
            f"tar -czvf {local_dir}/input_files.tar.gz -C {local_dir} input_files"
        )
