import shutil
import time
import logging
import os
from os import listdir
from os.path import isdir
from os.path import join as path_join
from database import Database
from gridpack import Gridpack
from ssh_executor import SSHExecutor


class Controller():

    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.last_tick = 0
        self.last_repository_tick = 0
        self.config = {}
        self.repository_tree = {}
        self.database = None
        self.gridpacks_to_reset = []
        self.gridpacks_to_delete = []
        self.ssh_executor = None

    def get_available_campaigns(self):
        """
        Get campaigns and campaign templates
        """
        tree = {}
        campaigns = [c for c in listdir('../campaigns') if isdir(path_join('../campaigns', c))]
        for name in campaigns:
            template_path = os.path.join('../campaigns', name, 'template')
            generators = [g for g in listdir(template_path) if isdir(path_join(template_path, g))]
            tree[name] = generators

        return tree

    def get_available_cards(self):
        tree = {}
        generators = [c for c in listdir('../cards') if isdir(path_join('../cards', c))]
        for generator in generators:
            generator_path = os.path.join('../cards', generator)
            processes = [p for p in listdir(generator_path) if isdir(path_join(generator_path, p))]
            for process in processes:
                process_path = os.path.join(generator_path, process)
                datasets = [d for d in listdir(process_path) if isdir(path_join(process_path, d))]
                tree.setdefault(generator, {})[process] = datasets

        return tree

    def update_repository_tree(self):
        self.repository_tree = {'campaigns': self.get_available_campaigns(),
                                'cards': self.get_available_cards()}
        self.last_repository_tick = int(time.time())

    def tick(self):
        self.logger.info('Controller tick start')
        tick_start = time.time()

        # Delete gridpacks
        if self.gridpacks_to_delete:
            self.logger.info('Gridpacks to delete: %s', ','.join(self.gridpacks_to_delete))
            for gridpack_id in self.gridpacks_to_delete:
                self.delete_gridpack(gridpack_id)

            self.gridpacks_to_delete = []
        
        if self.gridpacks_to_reset:
            # Reset gridpacks
            self.logger.info('Gridpacks to reset: %s', ','.join(self.gridpacks_to_reset))
            for gridpack_id in self.gridpacks_to_delete:
                self.reset_gridpack(gridpack_id)

            self.gridpacks_to_reset = []

        # Check gridpacks
        gridpacks_to_check = self.database.get_gridpacks_with_status('submitted,running,finishing')
        self.logger.info('Gridpacks to check: %s', ','.join(g['_id'] for g in gridpacks_to_check))
        for gridpack_json in gridpacks_to_check:
            gridpack = Gridpack(gridpack_json)
            self.check_condor_status(gridpack)
            condor_status = gridpack.get_condor_status()
            if condor_status in ('DONE', 'REMOVED'):
                # Refetch after check if running save
                self.collect_output(gridpack)

        # Submit gridpacks
        gridpacks_to_submit = self.database.get_gridpacks_with_status('new')
        self.logger.info('Gridpacks to submit: %s', ','.join(g['_id'] for g in gridpacks_to_submit))
        for gridpack_json in gridpacks_to_submit:
            gridpack = Gridpack(gridpack_json)
            status = gridpack.get_status()
            if status == 'new':
                # Double check and if it is new, submit it
                self.submit_to_condor(gridpack)

        tick_end = time.time()
        self.last_tick = int(tick_end)
        self.logger.info('Tick completed in %.2fs', tick_end - tick_start)

    def create(self, gridpack):
        """
        Add gridpack to the database 
        """
        gridpack_id = str(int(time.time() * 1000))
        gridpack.data['_id'] = gridpack_id
        gridpack.data['condor_id'] = 0
        gridpack.data['condor_status'] = '<unknown>'
        gridpack.data['status'] = 'new'
        self.database.create_gridpack(gridpack)
        self.logger.info('Gridpack %s was created', gridpack)
        return gridpack_id

    def reset(self, gridpack_id):
        self.logger.info('Adding %s to reset list', gridpack_id)
        self.gridpacks_to_reset.append(gridpack_id)

    def delete(self, gridpack_id):
        self.logger.info('Adding %s to delete list', gridpack_id)
        self.gridpacks_to_delete.append(gridpack_id)

    def reset_gridpack(self, gridpack_id):
        """
        Perform gridpack reset
        Terminate it in HTCondor and set to new so it would be submitted again
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            return

        gridpack = Gridpack(gridpack_json)
        self.terminate_gridpack(gridpack)
        gridpack.reset()
        self.database.update_gridpack(gridpack)

    def delete_gridpack(self, gridpack_id):
        """
        Terminate and delete gridpack
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            return

        gridpack = Gridpack(gridpack_json)
        self.terminate_gridpack(gridpack)
        self.database.delete_gridpack(gridpack)
        gridpack.rmdir()

    def terminate_gridpack(self, gridpack):
        """
        Terminate gridpack job in HTCondor
        """
        self.logger.info('Trying to terminate %s', gridpack)
        condor_id = gridpack.get_condor_id()
        if condor_id > 0:
            self.ssh_executor.execute_command(f'module load lxbatch/tzero && condor_rm {condor_id}')
        else:
            self.logger.info('Gridpack %s HTCondor id %s is not valid', gridpack, condor_id)

        self.logger.info('Finished terminating gridpack %s', gridpack)

    def submit_to_condor(self, gridpack):
        self.logger.info('Submitting %s', gridpack)
        gridpack.rmdir()
        gridpack.mkdir()

        gridpack.prepare_card_archive()
        gridpack.prepare_script()
        gridpack.prepare_jds_file()

        self.logger.info('Done preparing:\n%s', os.popen('ls -l %s' % (gridpack.local_dir())).read())
        # gridpack.rmdir()

    def set_config(self, config):
        self.config = config
        self.database = Database()
        self.ssh_executor = SSHExecutor(config)
