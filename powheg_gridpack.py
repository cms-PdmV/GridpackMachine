import os
import pathlib
from gridpack import Gridpack


class PowhegGridpack(Gridpack):

    def get_run_card(self):
        """
        Get cards from "Template" directory and customize them (run card)
        """
        dataset_dict = self.get_dataset_dict()
        campaign_dict = self.get_campaign_dict()
        templates_path = self.get_templates_path()
        template_name = dataset_dict['template']
        template_vars = dataset_dict.get('template_vars', [])
        template_vars['ebeam1'] = campaign_dict.get('beam', 0)
        template_vars['ebeam2'] = template_vars['ebeam1']
        template_vars.update(campaign_dict.get('template_vars', {}))
        input_file_name = os.path.join(templates_path, template_name)
        run_card = self.customize_file(input_file_name,
                                       dataset_dict.get('template_user', []),
                                       template_vars)

        return run_card

    def get_customize_card(self)
        """
        Get cards from "ModelParams" directory and customize them (customize card)
        """
        dataset_dict = self.get_dataset_dict()
        campaign_dict = self.get_campaign_dict()
        model_params_path = self.get_model_params_path()
        model_params_name = dataset_dict['model_params']
        model_params_vars = dataset_dict.get('model_params_vars', [])
        model_params_vars.update(campaign_dict.get('model_params_vars', {}))
        input_file_name = os.path.join(model_params_path, model_params_name)
        customize_card = self.customize_file(input_file_name,
                                             dataset_dict.get('model_params_user', []),
                                             model_params_vars)

        return customize_card

    def prepare_input_card(self):
        """
        Get run card and customize card, glue them together into input card and write it to job files dir
        """
        job_files_path = self.get_job_files_path()
        output_file_name = os.path.join(job_files_path, 'powheg.input')
        input_card = self.get_run_card() + '\n\n\n\n' + self.get_customize_card()
        self.logger.debug('Writing customized input card %s', output_file_name)
        self.logger.debug(input_card)
        with open(output_file_name, 'w') as output_file:
            output_file.write(input_card)

    def get_proc_card(self):
        """
        Create card with just the process name for proper Powheg gridpack
        production 
        """
        dataset_dict = self.get_dataset_dict()
        template_name = dataset_dict['template']
        return template_name.split('.', 1)[0]

    def prepare_proc_card(self):
        """
        Get proc card and write it to job files dir
        """
        job_files_path = self.get_job_files_path()
        output_file_name = os.path.join(job_files_path, 'process.dat')
        proc_card = self.get_proc_card()
        self.logger.debug('Writing proc card %s', output_file_name)
        self.logger.debug(proc_card)
        with open(output_file_name, 'w') as output_file:
            output_file.write(proc_card)

    def prepare_job_archive(self):
        """
        Make an archive with all necessary job files
        """
        job_files_path = self.get_job_files_path()
        pathlib.Path(job_files_path).mkdir(parents=True, exist_ok=True)
        self.prepare_input_card()
        self.prepare_proc_card()
        local_dir = self.local_dir()
        os.system(f'tar -czvf {local_dir}/input_files.tar.gz -C {local_dir} input_files')
