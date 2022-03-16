import os
import logging
import shutil
import pathlib
import json
import time
from copy import deepcopy
from config import Config
from utils import get_git_tags
from user import User


CORES = 8
MEMORY = CORES * 2000


class Gridpack():

    def __init__(self, data):
        self.logger = logging.getLogger()
        self.data = data

    def get_name(self):
        campaign = self.data['campaign']
        dataset = self.data['dataset']
        generator = self.data['generator']
        return f'{campaign}__{dataset}__{generator}'

    def validate(self):
        tags = get_git_tags(Config.get('gen_repository'), cache=True)
        genproductions = self.data['genproductions']
        if genproductions not in tags:
            return f'Bad GEN productions tag "{genproductions}"'

        beam = self.data['beam']
        if beam <= 0:
            return f'Bad beam "{beam}"'

        events = self.data['events']
        if events <= 0:
            return f'Bad events "{events}"'

        return None

    def reset(self):
        self.set_status('new')
        self.set_condor_status('')
        self.set_condor_id(0)

    def get_id(self):
        return self.data['_id']

    def get_status(self):
        return self.data['status']

    def set_status(self, status):
        """
        Setter for status
        """
        self.data['status'] = status

    def get_condor_status(self):
        return self.data['condor_status']

    def set_condor_status(self, condor_status):
        """
        Setter for condor status
        """
        self.data['condor_status'] = condor_status

    def get_condor_id(self):
        return self.data['condor_id']

    def set_condor_id(self, condor_id):
        """
        Setter for condor id
        """
        self.data['condor_id'] = condor_id

    def get_json(self):
        return deepcopy(self.data)

    def get_dataset_dict(self):
        """
        Return a dictionary from cards directory
        """
        if hasattr(self, 'dataset_dict'):
            return self.dataset_dict

        generator = self.data['generator']
        process = self.data['process']
        dataset_name = self.data['dataset']
        cards_path = os.path.join('..', 'cards', generator, process, dataset_name)
        dataset_dict_file = os.path.join(cards_path, f'{dataset_name}.json')
        self.logger.debug('Reading %s', dataset_dict_file)
        with open(dataset_dict_file) as input_file:
            dataset_dict = json.load(input_file)

        self.dataset_dict = dataset_dict
        return dataset_dict

    def mkdir(self):
        """
        Make local directory of gridpack
        """
        gridpack_id = self.get_id()
        local_directory = f'gridpacks/{gridpack_id}'
        pathlib.Path(local_directory).mkdir(parents=True, exist_ok=True)

    def rmdir(self):
        """
        Remove local directory of gridpack
        """
        gridpack_id = self.get_id()
        local_directory = f'gridpacks/{gridpack_id}'
        shutil.rmtree(local_directory, ignore_errors=True)

    def local_dir(self):
        """
        Return path to local directory of gridpack files
        """
        gridpack_id = self.get_id()
        return os.path.abspath(f'gridpacks/{gridpack_id}')

    def add_history_entry(self, entry):
        """
        Add a simple string history entry
        """
        user = User().get_username()
        timestamp = int(time.time())
        entry = entry.strip()
        self.data.setdefault('history', []).append({'user': user,
                                                    'time': timestamp,
                                                    'action': entry})

    def get_users(self):
        """
        Return a list of unique usernames of users in history
        """
        users = set(x['user'] for x in self.data['history'] if x['user'] != 'automatic')
        return sorted(list(users))

    def prepare_default_card(self):
        """
        Copy default cards to local directory
        """
        generator = self.data['generator']
        process = self.data['process']
        dataset_name = self.data['dataset']
        cards_path = os.path.join('..', 'cards', generator, process, dataset_name)
        local_cards_path = os.path.join(self.local_dir(), 'cards')
        pathlib.Path(local_cards_path).mkdir(parents=True, exist_ok=True)
        self.logger.debug('Copying %s/*.dat to %s', cards_path, local_cards_path)
        os.system(f'cp {cards_path}/*.dat {local_cards_path}')

    def prepare_run_card(self):
        """
        Copy cards from campaign template directory to local directory
        """
        campaign = self.data['campaign']
        generator = self.data['generator']
        dataset_name = self.data['dataset']
        tamplate_path = os.path.join('..', 'campaigns', campaign, 'template', generator, 'run_card')
        run_card_file_path = os.path.join(self.local_dir(), 'cards', f'{dataset_name}_run_card.dat')
        if "amcatnlo" in tamplate_path.lower():
            os.system(f"cp {tamplate_path}/NLO_run_card.dat {run_card_file_path}")
        elif "madgraph" in tamplate_path.lower():
            os.system(f"cp {tamplate_path}/LO_run_card.dat {run_card_file_path}")
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
        Copy cards from "scheme" directory and customize them
        """
        dataset_dict = self.get_dataset_dict()
        scheme_name = dataset_dict.get('scheme')
        if not scheme_name:
            return

        campaign = self.data['campaign']
        generator = self.data['generator']
        dataset_name = self.data['dataset']
        scheme_file = os.path.join('..', 'campaigns', campaign, 'template', generator, 'scheme', scheme_name)
        customized_file = os.path.join(self.local_dir(), 'cards',  f'{dataset_name}_customizecards.dat')
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

    def prepare_card_archive(self):
        """
        Make an archive with all necessary card files
        """
        self.prepare_default_card()
        self.prepare_run_card()
        self.prepare_customize_card()
        local_dir = self.local_dir()
        os.system(f"tar -czvf {local_dir}/cards.tar.gz -C {local_dir} cards")

    def prepare_script(self):
        """
        Make a bash script that will run in condor
        """
        repository = Config.get('gen_repository')
        generator = self.data['generator']
        dataset_name = self.data['dataset']
        genproductions = self.data['genproductions']
        gen_archive = f"https://github.com/{repository}/archive/refs/tags/{genproductions}.tar.gz"
        command = ['#!/bin/sh',
                'export HOME=$(pwd)',
                'export ORG_PWD=$(pwd)',
                f'wget {gen_archive} -O gen_productions.tar.gz',
                'tar -xzf gen_productions.tar.gz',
                'rm gen_productions.tar.gz',
                f'mv genproductions-{genproductions} genproductions',
                f'mv cards.tar.gz genproductions/bin/{generator}',
                f'cd genproductions/bin/{generator}',
                'tar -xzf cards.tar.gz',
                f'./gridpack_generation.sh {dataset_name} cards',
                f'mv {dataset_name}*.xz $ORG_PWD']

        script_name = f'GRIDPACK_{self.get_id()}.sh'
        script_path = os.path.join(self.local_dir(), script_name)
        self.logger.debug('Writing sh script to %s', script_path)
        with open(script_path, 'w') as script_file:
            script_file.write('\n'.join(command))

        os.system(f"chmod a+x {script_path}")

    def prepare_jds_file(self):
        """
        Make condor job description file
        """
        gridpack_id = self.get_id()
        script_name = f'GRIDPACK_{gridpack_id}.sh'
        jds = [f'executable              = {script_name}',
               'transfer_input_files    = cards.tar.gz',
               'when_to_transfer_output = on_exit',
               'should_transfer_files   = yes',
               '+JobFlavour             = "testmatch"',
               'output                  = log.out',
               'error                   = log.err',
               'log                     = job_log.log',
               f'RequestCpus             = {CORES}',
               f'RequestMemory           = {MEMORY}',
               '+accounting_group       = highprio',
               '+AccountingGroup        = "highprio.pdmvserv"',
               '+AcctGroup              = "highprio"',
               '+AcctGroupUser          = "pdmvserv"',
               '+DESIRED_Sites          = "T2_CH_CERN"',
               '+REQUIRED_OS            = "rhel7"',
               'leave_in_queue          = JobStatus == 4 && (CompletionDate =?= UNDEFINED || ((CurrentTime - CompletionDate) < 7200))',
               '+CMS_Type               = "test"',
               '+CMS_JobType            = "PdmVGridpack"',
               '+CMS_TaskType           = "PdmVGridpack"',
               '+CMS_SubmissionTool     = "Condor_SI"',
               '+CMS_WMTool             = "Condor_SI"',
               'queue']

        jds_name = f'GRIDPACK_{gridpack_id}.jds'
        jds_path = os.path.join(self.local_dir(), jds_name)
        self.logger.debug('Writing JDS to %s', jds_path)
        with open(jds_path, 'w') as jds_file:
            jds_file.write('\n'.join(jds))
