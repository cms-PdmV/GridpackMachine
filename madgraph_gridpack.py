import os
import glob
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
        if glob.glob(f'{cards_path}/*_cuts.f'):
            os.system(f'cp {cards_path}/*_cuts.f {job_files_path}')

    def get_run_card(self):
        """
        Get cards from "Template" directory and customize them
        """
        dataset_dict = self.get_dataset_dict()
        campaign_dict = self.get_campaign_dict()
        templates_path = self.get_templates_path()
        template_name = dataset_dict['template']
        replace_vars = dataset_dict.get('template_vars', [])
        replace_vars['ebeam1'] = campaign_dict.get('beam', 0)
        replace_vars['ebeam2'] = replace_vars['ebeam1']
        replace_vars.update(campaign_dict.get('template_vars', {}))
        input_file_name = os.path.join(templates_path, template_name)
        run_card = self.customize_file(input_file_name,
                                       dataset_dict.get('template_user', []),
                                       replace_vars)
        return run_card

    def prepare_run_card(self):
        """
        Get run card and write it to job files dir
        """
        job_files_path = self.get_job_files_path()
        dataset_name = self.data['dataset']
        output_file_name = os.path.join(job_files_path, f'{dataset_name}_run_card.dat')
        run_card = self.get_run_card()
        self.logger.debug('Writing customized run card %s', output_file_name)
        self.logger.debug(run_card)
        with open(output_file_name, 'w') as output_file:
            output_file.write(run_card)

    def get_customize_card(self):
        """
        Get cards from "ModelParams" directory and customize them
        """
        dataset_dict = self.get_dataset_dict()
        campaign_dict = self.get_campaign_dict()
        model_params_path = self.get_model_params_path()
        model_params_name = dataset_dict['model_params']
        replace_vars = dataset_dict.get('model_params_vars', [])
        replace_vars.update(campaign_dict.get('model_params_vars', {}))
        input_file_name = os.path.join(model_params_path, model_params_name)
        customize_card = self.customize_file(input_file_name,
                                             dataset_dict.get('model_params_user', []),
                                             replace_vars)
        return customize_card

    def prepare_customize_card(self):
        """
        Get customize card and write it to job files dir
        """
        job_files_path = self.get_job_files_path()
        dataset_name = self.data['dataset']
        output_file_name = os.path.join(job_files_path, f'{dataset_name}_customizecards.dat')
        customize_card = self.get_customize_card()
        self.logger.debug('Writing customized card %s', output_file_name)
        self.logger.debug(customize_card)
        with open(output_file_name, 'w') as output_file:
            output_file.write(customize_card)

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
