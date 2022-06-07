import os
import json
import logging
from config import Config
from utils import get_indentation


class FragmentBuilder():

    def __init__(self):
        self.logger = logging.getLogger()
        files_dir = Config.get('gridpack_files_path')
        self.fragments_path = os.path.join(files_dir, 'Fragments')
        self.templates_path = os.path.join(self.fragments_path, 'Templates')
        self.imports_path = os.path.join(self.fragments_path, 'imports.json')

    def build_fragment(self, gridpack):
        lhe = bool('lhe' in gridpack.get('campaign').lower())
        # Build the fragment
        fragment = 'import FWCore.ParameterSet.Config as cms\n\n'
        if lhe:
            self.logger.debug('Adding external LHE producer')
            fragment += self.get_external_lhe_producer()

        generator = gridpack.get('dataset').split('_')[-1]
        fragment += self.get_hadronizer(generator)
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
        concurrent = dataset_dict.get('concurrent', True)
        self.logger.debug('Concurrent: %s', concurrent)
        replace = {}
        if concurrent:
            replace["__generateConcurrently__"] = "generateConcurrently = cms.untracked.bool(True),"
            replace["__concurrent__"] = "Concurrent"
        else:
            replace["__generateConcurrently__"] = ""
            replace["__concurrent__"] = ""

        tune = gridpack.get('tune')
        replace["__tuneName__"] = tune
        replace["__tuneImport__"] = import_dict['tune'][tune]
        beam_energy = gridpack.get('beam_energy')
        replace["__comEnergy__"] = str(int(beam_energy) * 2)
        process_parameters_space = ' ' * get_indentation("__processParameters__", fragment)
        process_parameters = dataset_dict.get('process_parameters', [])
        process_parameters = [f"{process_parameters_space}'{p}'," for p in process_parameters]
        replace["__processParameters__"] = '\n'.join(process_parameters).strip()
        self.logger.debug('Replace %s', json.dumps(replace, indent=2))

        for key, value in replace.items():
            fragment = fragment.replace(key, value)

        return fragment
