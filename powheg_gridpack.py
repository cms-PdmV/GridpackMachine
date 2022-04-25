import os
import pathlib
from gridpack import Gridpack


class PowhegGridpack(Gridpack):

    def prepare_powheg_card(self):
        """
        Create Powheg cards from templates and user specific input
        """
        self.logger.debug('Start preparing powheg card')
        # Fill process file with content of process specific template and adjust
        # settings like beam energy
        dataset_dict = self.get_dataset_dict()
        if not 'powheg_process' in dataset_dict:
            self.logger.error('Could not find powheg_process block in dataset dictionary')
            raise Exception()

        powheg_process = dataset_dict['powheg_process']
        template_path = self.get_templates_path()
        process_template_path = os.path.join(template_path, f'{powheg_process}.input')
        if not os.path.exists(process_template_path):
            self.logger.error('Could not find process template %s', process_template_path)
            raise Exception()

        with open(process_template_path) as input_file:
            self.logger.debug('Reading %s...', process_template_path)
            process_file = input_file.read()

        beam = str(self.data['beam'])
        # TODO: sync strategy for PDF and fix pdf input 
        pdf = '325300'
        process_file = process_file.replace('$ebeam1', beam)
        process_file = process_file.replace('$ebeam2', beam)
        process_file = process_file.replace('$pdf1', pdf)
        process_file = process_file.replace('$pdf2', pdf)

        # Append campaign specific settings for process 
        model_params_path = self.get_model_params_path()
        model_params_file_path = os.path.join(model_params_path, f'{powheg_process}.input')
        if not os.path.exists(model_params_file_path):
            self.logger.error('Could not find model parameters %s', model_params_file_path)
            raise Exception()

        with open(model_params_file_path) as input_file:
            self.logger.debug('Reading %s...', model_params_file_path)
            model_params_file = input_file.read() + '\n'

        process_file += model_params_file + '\n'
        # Append user specific settings 
        for user_line in dataset_dict.get('user', []):
            self.logger.debug('Appeding %s', user_line)
            process_file += user_line + '\n'

        # Write to local powheg steering file 
        job_files_path = self.get_job_files_path()
        powheg_steering_file_path = os.path.join(job_files_path, 'powheg.input')
        with open(powheg_steering_file_path, 'w') as output_file:
            self.logger.debug('Writing %s...', powheg_steering_file_path)
            output_file.write(process_file)

    def prepare_procname_card(self):
        """
        Create card with just the process name for proper Powheg gridpack
        production 
        """
        self.logger.debug('Preparing card with Powheg process name')
        job_files_path = self.get_job_files_path()
        dataset_dict = self.get_dataset_dict()
        powheg_process = dataset_dict['powheg_process']
        powheg_process_name_file = os.path.join(job_files_path, 'process.dat')
        with open(powheg_process_name_file, 'w') as output_file:
            self.logger.debug('Writing %s...', powheg_process_name_file)
            output_file.write(powheg_process)
            output_file.write('\n')

    def prepare_job_archive(self):
        """
        Make an archive with all necessary job files
        """
        job_files_path = self.get_job_files_path()
        pathlib.Path(job_files_path).mkdir(parents=True, exist_ok=True)
        self.prepare_powheg_card()
        self.prepare_procname_card()
        local_dir = self.local_dir()
        os.system(f'tar -czvf {local_dir}/input_files.tar.gz -C {local_dir} input_files')
