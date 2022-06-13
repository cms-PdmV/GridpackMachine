import os
import pathlib
from gridpack import Gridpack


class MadgraphGridpack(Gridpack):

    def prepare_default_card(self):
        """
        Copy default cards to local directory
        """
        cards_path = self.get_cards_path()
        job_files_path = self.get_job_files_path()
        self.logger.debug('Copying %s/*.dat to %s', cards_path, job_files_path)
        os.system(f'cp {cards_path}/*.dat {job_files_path}')

    def prepare_run_card(self):
        """
        Copy cards from "Template" directory and customize them
        """
        dataset_dict = self.get_dataset_dict()
        template_name = dataset_dict.get('template')
        dataset_name = self.data['dataset']
        input_file_name = os.path.join(self.get_templates_path(), template_name)
        job_files_path = self.get_job_files_path()
        output_file_name = os.path.join(job_files_path, f'{dataset_name}_run_card.dat')
        self.customize_file(input_file_name,
                            dataset_dict.get('template_user', []),
                            dataset_dict.get('template_vars', []),
                            output_file_name)

    def prepare_customize_card(self):
        """
        Copy cards from "ModelParams" directory and customize them
        """
        dataset_dict = self.get_dataset_dict()
        model_params_name = dataset_dict.get('model_params')
        dataset_name = self.data['dataset']
        input_file_name = os.path.join(self.get_model_params_path(), model_params_name)
        job_files_path = self.get_job_files_path()
        output_file_name = os.path.join(job_files_path, f'{dataset_name}_customizecards.dat')
        self.customize_file(input_file_name,
                            dataset_dict.get('model_params_user', []),
                            dataset_dict.get('model_params_vars', []),
                            output_file_name)

    def prepare_job_archive(self):
        """
        Make an archive with all necessary job files
        """
        job_files_path = self.get_job_files_path()
        pathlib.Path(job_files_path).mkdir(parents=True, exist_ok=True)
        self.prepare_default_card()
        self.prepare_run_card()
        self.prepare_customize_card()
        local_dir = self.local_dir()
        os.system(f'tar -czvf {local_dir}/input_files.tar.gz -C {local_dir} input_files')
