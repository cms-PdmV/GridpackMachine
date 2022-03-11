from copy import deepcopy
import logging


class Gridpack():

    def __init__(self, data):
        self.logger = logging.getLogger('logger')
        self.data = data

    def get_name(self):
        campaign = self.data['campaign']
        dataset = self.data['dataset']
        generator = self.data['generator']
        return f'{campaign}__{dataset}__{generator}'

    def validate(self):
        pass

    def get_status(self):
        return self.data['status']

    def get_condor_status(self):
        return self.data['condor_status']

    def get_condor_id(self):
        return self.data['condor_id']

    def get_json(self):
        return deepcopy(self.data)