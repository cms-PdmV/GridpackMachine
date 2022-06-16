import os
import json
import logging
from config import Config
from utils import get_indentation
from os.path import join as path_join


class FragmentBuilder():

    def __init__(self):
        self.logger = logging.getLogger()
        files_dir = Config.get('gridpack_files_path')
        self.fragments_path = os.path.join(files_dir, 'Fragments')
        self.imports_path = os.path.join(self.fragments_path, 'imports.json')

    def build_fragment(self, gridpack):
        lhe = bool('lhe' in gridpack.get('campaign').lower())
        # Build the fragment
        fragment = 'import FWCore.ParameterSet.Config as cms\n\n'
        if lhe:
            self.logger.debug('Adding external LHE producer')
            fragment += self.get_external_lhe_producer()

        dataset_dict = gridpack.get_dataset_dict()
        file_list = dataset_dict.get('fragment', [])
        if isinstance(file_list, str):
            file_list = [file_list]

        self.logger.info('List of files for fragment builder: %s', ','.join(file_list))
        fragment = ''
        for file_name in file_list:
            with open(path_join(self.fragments_path, file_name)) as input_file:
                contents = input_file.read().strip()

            fragment += contents + '\n\n'

        fragment = self.fragment_replace(fragment, gridpack)
        return fragment

    def get_external_lhe_producer(self):
        path = os.path.join('Templates', 'ExternalLHEProducer.dat')
        if not os.path.exists(path):
            raise Exception(f'Could not find {path} as external LHE producer')

        self.logger.debug('Reading %s', path)
        with open(path) as input_file:
            contents = input_file.read()

        return '%s\n' % (contents.strip())  # Add newline to the end of the contents

    def get_hadronizer(self, generator):
        path = os.path.join(self.templates_path, f'{generator}.dat')
        if not os.path.exists(path):
            raise Exception(f'Could not find {path} as hadronizer')

        self.logger.debug('Reading %s', path)
        with open(path) as input_file:
            contents = input_file.read()

        return '%s\n' % (contents.strip())  # Add newline to the end of the contents

    def fragment_replace(self, fragment, gridpack):
        with open(self.imports_path) as input_file:
            import_dict = json.load(input_file)

        dataset_dict = gridpack.get_dataset_dict()
        campaign_dict = gridpack.get_campaign_dict()
        fragment_vars = dataset_dict.get('fragment_vars', [])
        fragment_vars.update(campaign_dict.get('fragment_vars', {}))
        tune = gridpack.get('tune')
        fragment_vars['tuneName'] = tune
        fragment_vars['tuneImport'] = import_dict['tune'][tune]
        for key, value in fragment_vars.items():
            if isinstance(value, list):
                indentation = ' ' * get_indentation(f'${key}', fragment)
                value = [f'{indentation}{x}' for x in value]
                value = ',\n'.join(value).strip()

            fragment = fragment.replace(f'${key}', str(value))

        return fragment
