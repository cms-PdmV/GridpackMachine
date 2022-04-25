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
        Copy cards from campaign template directory to local directory
        """
        dataset_name = self.data['dataset']
        template_path = self.get_templates_path()
        job_files_path = self.get_job_files_path()
        run_card_file_path = os.path.join(job_files_path, f'{dataset_name}_run_card.dat')
        if dataset_name.rsplit("_", 1)[1].startswith("amcatnlo"):
            os.system(f"cp {template_path}/NLO_run_card.dat {run_card_file_path}")
        elif dataset_name.rsplit("_", 1)[1].startswith("madgraph"):
            os.system(f"cp {template_path}/LO_run_card.dat {run_card_file_path}")
        else:
            self.logger.error('Could not find "amcatnlo" or "madgraph" in "%s"', dataset_name)
            raise Exception()

        with open(run_card_file_path) as input_file:
            self.logger.debug('Reading %s...', run_card_file_path)
            run_card_file = input_file.read()

        dataset_dict = self.get_dataset_dict()
        beam = str(self.data['beam'])
        run_card_file = run_card_file.replace('$ebeam1', beam)
        run_card_file = run_card_file.replace('$ebeam2', beam)
        for key, value in dataset_dict.get('run_card', {}).items():
            key = f'${key}'
            self.logger.debug('Replacing "%s" with "%s" in %s', key, value, run_card_file_path)
            run_card_file = run_card_file.replace(key, value)

        with open(run_card_file_path, 'w') as output_file:
            self.logger.debug('Writing %s...', run_card_file_path)
            output_file.write(run_card_file)

    def prepare_customize_card(self):
        """
        Copy cards from "ModelParams" directory and customize them
        """
        dataset_dict = self.get_dataset_dict()
        scheme_name = dataset_dict.get('scheme')
        if not scheme_name:
            return

        dataset_name = self.data['dataset']
        scheme_file = os.path.join(self.get_model_params_path(), scheme_name)
        job_files_path = self.get_job_files_path()
        customized_file = os.path.join(job_files_path, f'{dataset_name}_customizecards.dat')
        self.logger.debug('Reading scheme file %s', scheme_file)
        with open(scheme_file) as scheme_file:
            scheme = scheme_file.read()

        scheme = scheme.split('\n')
        scheme += ['', '# User settings']
        for user_line in dataset_dict.get('user', []):
            self.logger.debug('Appeding %s', user_line)
            scheme += [user_line]

        scheme = '\n'.join(scheme)
        self.logger.debug('Writing customized scheme file %s', customized_file)
        with open(customized_file, 'w') as scheme_file:
            scheme_file.write(scheme)

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
