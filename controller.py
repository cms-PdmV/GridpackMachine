import time
import logging
import os
import zipfile
import pathlib
import traceback
from typing import Optional, Union
from database import Database
from gridpack import Gridpack
from email_sender import EmailSender
from utils import (clean_split,
                   get_available_campaigns,
                   get_available_cards,
                   get_git_branches,
                   get_available_tunes,
                   get_jobs_in_condor,
                   get_latest_log_output_in_condor,
                   retrieve_all_files_available,
                   run_command)
from environment import (GRIDPACK_FILES_PATH,
                         GEN_REPOSITORY,
                         SUBMISSION_HOST,
                         SERVICE_ACCOUNT_USERNAME,
                         SERVICE_ACCOUNT_PASSWORD,
                         REMOTE_DIRECTORY,
                         TICKETS_DIRECTORY,
                         PRODUCTION,
                         SERVICE_URL,
                         EMAIL_AUTH)
from ssh_executor import SSHExecutor, HTCondorExecutor
from threading import Lock
from fragment_builder import FragmentBuilder


class Controller():

    def __init__(self):
        self.logger = logging.getLogger()
        self.last_tick = 0
        self.last_repository_tick = 0
        self.repository_tree = {}
        self.database = Database()
        self.gridpacks_to_reset = []
        self.gridpacks_to_approve = []
        self.gridpacks_that_reuse_output = []
        self.gridpacks_to_delete = []
        self.gridpacks_to_create_requests = []
        self.repository_tick_pause = 60
        self.tick_lock = Lock()
        self.job_cores = [1, 2, 4, 8, 16, 32, 64]
        self.job_memory = [cores * 1000 for cores in self.job_cores]

    def update_repository_tree(self):
        now = int(time.time())
        if now - self.repository_tick_pause < self.last_repository_tick:
            self.logger.info('Not updating repository, last update happened recently')
            return

        branches = get_git_branches(GEN_REPOSITORY, cache=False)
        branches = branches[::-1]
        files_dir = GRIDPACK_FILES_PATH
        run_command([f'cd {files_dir}',
                     'git pull'])
        self.repository_tree = {'campaigns': get_available_campaigns(cache=False),
                                'cards': get_available_cards(cache=False),
                                'branches': branches,
                                'tunes': get_available_tunes(cache=False)}
        self.last_repository_tick = int(time.time())

    def tick(self):
        with self.tick_lock:
            self.logger.info('Controller tick start')
            tick_start = time.time()
            self.internal_tick()
            tick_end = time.time()
            self.last_tick = int(tick_end)
            self.logger.info('Tick completed in %.2fs', tick_end - tick_start)
            # Three second cooldown
            time.sleep(3)

    def internal_tick(self):
        # Delete gridpacks
        if self.gridpacks_to_delete:
            self.logger.info('Gridpacks to delete: %s', ','.join(self.gridpacks_to_delete))
            for gridpack_id in self.gridpacks_to_delete:
                self.delete_gridpack(gridpack_id)

            self.gridpacks_to_delete = []

        if self.gridpacks_to_reset:
            # Reset gridpacks
            self.logger.info('Gridpacks to reset: %s', ','.join(self.gridpacks_to_reset))
            for gridpack_id in self.gridpacks_to_reset:
                self.reset_gridpack(gridpack_id)

            self.gridpacks_to_reset = []

        if self.gridpacks_that_reuse_output:
            # Check Gridpacks that could reuse output
            self.logger.info(
                'Gridpacks that could reuse output - Checking them: %s', 
                ','.join(self.gridpacks_that_reuse_output)
            )
            with SSHExecutor(
                SUBMISSION_HOST, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
            ) as ssh:
                for gridpack_id in self.gridpacks_that_reuse_output:
                    self.reuse_gridpack(
                        gridpack_id=gridpack_id,
                        ssh_session=ssh
                    )

            self.gridpacks_that_reuse_output = []

        if self.gridpacks_to_approve:
            # Approve gridpacks
            self.logger.info('Gridpacks to approve: %s', ','.join(self.gridpacks_to_approve))
            for gridpack_id in self.gridpacks_to_approve:
                self.approve_gridpack(gridpack_id)

            self.gridpacks_to_approve = []

        # Check gridpacks
        gridpacks_to_check = self.database.get_gridpacks_with_status('submitted,running,finishing')
        self.logger.info('Gridpacks to check: %s', ','.join(g['_id'] for g in gridpacks_to_check))
        condor_jobs = {}
        if gridpacks_to_check:
            with HTCondorExecutor(
                SUBMISSION_HOST, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
            ) as ssh:
                condor_jobs = get_jobs_in_condor(ssh)

        for gridpack_json in gridpacks_to_check:
            gridpack = Gridpack.make(gridpack_json)
            self.update_condor_status(gridpack, condor_jobs)
            condor_status = gridpack.get_condor_status()
            if condor_status in ('DONE', 'REMOVED'):
                # Refetch after check if running save
                self.collect_output(gridpack)
            if condor_status in ('RUN'):
                # Stream the output to a public area
                with HTCondorExecutor(
                    SUBMISSION_HOST, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
                ) as ssh:
                    get_latest_log_output_in_condor(gridpack=gridpack, ssh=ssh)

        if self.gridpacks_to_create_requests:
            # Approve gridpacks
            self.logger.info('Gridpacks to create requests: %s', ','.join(self.gridpacks_to_create_requests))
            for gridpack_id in self.gridpacks_to_create_requests:
                self.create_request_for_gridpack(gridpack_id)

            self.gridpacks_to_create_requests = []

        # Submit gridpacks
        gridpacks_to_submit = self.database.get_gridpacks_with_status('approved')
        self.logger.info('Gridpacks to submit: %s', ','.join(g['_id'] for g in gridpacks_to_submit))
        for gridpack_json in gridpacks_to_submit:
            gridpack = Gridpack.make(gridpack_json)
            status = gridpack.get_status()
            if status == 'approved':
                # Double check and if it is approved, submit it
                self.submit_to_condor(gridpack)

    def create(self, gridpack):
        """
        Add gridpack to the database
        """
        gridpack_id = str(int(time.time() * 1000))
        gridpack.data['_id'] = gridpack_id
        gridpack.data['store_into_subfolders'] = True
        gridpack.reset()
        gridpack.data['history'] = []
        gridpack.add_history_entry('created')
        self.database.create_gridpack(gridpack)
        self.logger.info('Gridpack %s was created', gridpack)
        return gridpack_id

    def reset(self, gridpack_id):
        self.logger.info('Adding %s to reset list', gridpack_id)
        self.gridpacks_to_reset.append(gridpack_id)

    def create_request(self, gridpack_id):
        self.logger.info('Adding %s to create request list', gridpack_id)
        self.gridpacks_to_create_requests.append(gridpack_id)

    def approve(self, gridpack_id):
        submit_or_reuse: Optional[bool] = self.submit_or_reuse_gridpack(gridpack_id)
        if submit_or_reuse is None:
            self.logger.info(
                'Skip Gridpack %s because there were issues that impede '
                'reusing old output files',
                gridpack_id
            )
            return 
        if not submit_or_reuse:
            self.logger.info('Checking if Gridpack %s can reuse old artifacts', gridpack_id)
            self.gridpacks_that_reuse_output.append(gridpack_id)
        else:
            self.logger.info('Adding %s to approve list', gridpack_id)
            self.gridpacks_to_approve.append(gridpack_id)

    def delete(self, gridpack_id):
        self.logger.info('Adding %s to delete list', gridpack_id)
        self.gridpacks_to_delete.append(gridpack_id)

    def submit_or_reuse_gridpack(self, gridpack_id):
        """
        Determines if this Gridpack request can reuse 
        the Gridpack results related to old requests or 
        if it should run a batch job 
        to create it for the first time.

        Args:
            gridpack_id (str): ID for a Gridpack which already exists
                and its going to be checked.

        Returns:
            bool | None: True if it is required to submit a batch job
                to create the Gridpack. False if it is possible
                to reuse an old one. None if it was requested to reuse
                a Gridpack but there is an error that impedes this.
        """
        gridpack_json: dict = self.database.get_gridpack(gridpack_id)
        gridpack: Gridpack = Gridpack.make(gridpack_json)
        self.logger.info(
            "Checking if reuse or submit Gridpack: %s",
            gridpack
        )        
        try:
            _ = gridpack.get_reusable_gridpack_path()
            return False
        except AssertionError as ae:
            self.logger.info(ae)
            return True
        except ValueError as va:
            self.__process_failed_reuse(
                gridpack=gridpack,
                error=va
            )
        return None

    def reset_gridpack(self, gridpack_id):
        """
        Perform gridpack reset
        Terminate it in HTCondor and set to new so it would be submitted again
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            self.logger.error('Cannot reset %s because it is not in database', gridpack_id)
            return

        gridpack = Gridpack.make(gridpack_json)
        self.logger.info('Reseting %s', gridpack)
        self.terminate_gridpack(gridpack)
        gridpack.reset()
        gridpack.add_history_entry('reset')
        self.database.update_gridpack(gridpack)

    def approve_gridpack(self, gridpack_id):
        """
        Approve a gridpack
        Terminate it in HTCondor and set to new so it would be submitted again
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            self.logger.error('Cannot reset %s because it is not in database', gridpack_id)
            return

        gridpack = Gridpack.make(gridpack_json)
        self.logger.info('Approving %s', gridpack)
        gridpack.set_status('approved')
        gridpack.add_history_entry('approve')
        self.database.update_gridpack(gridpack)

    def reuse_gridpack(self, gridpack_id: str, ssh_session: SSHExecutor):
        """
        Scan the Gridpack output folder and choose one artifact
        to avoid executing a batch job. If there are not available Gridpacks
        in the output folder, append this job to execute a 
        batch job to create it.

        Args:
            gridpack_id (str): Gridpack ID.
            ssh_session (SSHExecutor): Session to a remote host.
        """
        gridpack_json: dict = self.database.get_gridpack(gridpack_id)
        gridpack: Gridpack = Gridpack.make(gridpack_json)
        self.logger.info(
            "Checking output gridpacks to reuse for Gridpack: %s",
            gridpack
        )
        try:
            reuse_gridpack_in: pathlib.Path = gridpack.get_reusable_gridpack_path()
            reuse_gridpack_folder: str = str(reuse_gridpack_in.parent)
            possible_gridpacks = retrieve_all_files_available(
                folders=[reuse_gridpack_in],
                ssh_session=ssh_session
            )
            self.logger.info("Possible Gridpack files to reuse: %s", possible_gridpacks)
            gridpack_options = possible_gridpacks.get(reuse_gridpack_folder, [])
            if not gridpack_options:
                cause = (
                    "There are no Gridpacks to reuse in the "
                    f"target folder: {reuse_gridpack_folder} "
                    f"whose file name complies with the regex: {reuse_gridpack_in.name}"
                )
                raise AssertionError(cause)
            
            # Currently, just take the first, most recent, file
            gridpack_file_metadata: dict = gridpack_options[0]
            gridpack_file: pathlib.Path = gridpack_file_metadata.get("file_path")
            
            # Find the Gridpack related to the output file
            requested_archive: str = gridpack_file.name
            requested_campaign: str = gridpack.get("campaign")
            requested_generator: str = gridpack.get("generator")
            requested_process: str = reuse_gridpack_in.parent.name
            
            related_gridpacks = self.database.get_gridpacks_by_archive(
                archive=requested_archive,
                campaign=requested_campaign,
                generator=requested_generator,
                process=requested_process
            )
            if not related_gridpacks:
                # 'gridpack_reused' := '-1'
                # There's a valid file but the record about the Gridpack
                # request that produced it couldn't be found.
                cause = (
                    "Could not find the parent Gridpack that create the "
                    f"output file: {gridpack_file}"
                )
                self.logger.warning(cause)
                gridpack.data['gridpack_reused'] = "-1"
            else:
                parent_gridpack: Gridpack = Gridpack.make(related_gridpacks[0])
                gridpack.data['gridpack_reused'] = parent_gridpack.get_id()

            # Set the gridpack artifact
            # Set the archive name just as a reference for the table
            gridpack.data['archive_absolute'] = str(gridpack_file)
            gridpack.data['archive'] = str(gridpack_file.name)
            gridpack.set_status('reused')
            gridpack.add_history_entry(f'gridpack reused')
            gridpack.delete_cores_memory()
            self.database.update_gridpack(gridpack)
            
            # Create a McM request for it
            self.gridpacks_to_create_requests.append(gridpack_id)
            self.send_reused_notification(gridpack=gridpack)

        except Exception as e:
            self.__process_failed_reuse(
                gridpack=gridpack,
                error=e
            )

    def __process_failed_reuse(
            self,
            gridpack: Gridpack, 
            error: Union[str, Exception]
        ):
        """
        In case a Gridpack fails, send an email notification
        and update its status as failed.
        """
        error_message: str = "Unable to reuse Gridpacks - "
        error_message += f"Cause: {error}" if isinstance(error, str) else f"Error: {error}"
        self.logger.error(error_message, exc_info=True)
        gridpack.set_status('failed')
        gridpack.add_history_entry(f'reuse failed')
        gridpack.delete_cores_memory()
        self.database.update_gridpack(gridpack)
        self.send_failed_reused_notification(
            gridpack=gridpack,
            cause=error_message
        )

    def create_request_for_gridpack(self, gridpack_id):
        """
        Create request for a gridpack in McM
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            self.logger.error('Cannot reset %s because it is not in database', gridpack_id)
            return

        gridpack = Gridpack.make(gridpack_json)
        self.logger.info('Creating request for %s', gridpack)
        gridpack.add_history_entry('create request')
        self.create_mcm_request(gridpack)
        self.database.update_gridpack(gridpack)

    def force_request_for_gridpack(self, gridpack_id):
        """
        Forces the creation for a request into McM for a Gridpack

        Returns:
            dict: If there is an invalid precondition for forcing a request for
                a Gridpack
            None: If the force process finish successfully
        """
        gridpack_json = self.database.get_gridpack(gridpack_id=gridpack_id)
        if not gridpack_json:
            msg = 'Cannot force a request for %s because it is not in database' % gridpack_id
            self.logger.error(msg)
            return {'message': msg}

        gridpack = Gridpack.make(gridpack_json)
        if gridpack.get_status() != 'done':
            msg = ('Cannot force a request for %s because its status is not done' % gridpack_id)
            self.logger.error(msg)
            return {'message': msg}
        
        if gridpack.get('prepid'):
            msg = ('Cannot force a request for %s because it has already a valid request in McM' % gridpack_id)
            self.logger.error(msg)
            return {'message': msg}
        
        self.logger.info('Forcing a request creation for %s', gridpack)
        gridpack.add_history_entry('force request')
        self.create_mcm_request(gridpack)
        self.database.update_gridpack(gridpack)
        return None
    
    def get_original_gridpack(self, gridpack_id: str):
        """
        Returns the requested Gridpack or the information of the Gridpack
        that submitted the batch job to the create the Gridpack artifact
        for Gridpacks that reused output from another.

        Args:
            gridpack_id (str): Gridpack to retrieve the run card.
        Returns:
            Gridpack: Requested gridpack or its parent.
        Raises:
            ValueError: If there is no Gridpack linked with the
                provided ID.
            AssertionError: If the Gridpack data indicates that it reused
                output but there was not possible to find the Gridpack
                that submit the job.
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            raise ValueError(
                f"There is no Gridpack linked to the ID: {gridpack_id}"
            )
        
        gridpack: Gridpack = Gridpack.make(gridpack_json)
        original_id: str = gridpack.get_gridpack_reused()
        if not original_id:
            return gridpack

        # This Gridpacks reuses output from another
        original_gridpack_json = self.database.get_gridpack(original_id)
        if not original_gridpack_json:
            error_message = (
                "Could not retrieve the data "
                "for the original Gridpack that "
                "performed the submission.\n"
                f"Gridpack ID: {gridpack.get_id()}"
            )
            raise AssertionError(error_message)
        
        original_gridpack: Gridpack = Gridpack.make(original_gridpack_json)
        return original_gridpack

    def delete_gridpack(self, gridpack_id):
        """
        Terminate and delete gridpack
        """
        gridpack_json = self.database.get_gridpack(gridpack_id)
        if not gridpack_json:
            return

        gridpack = Gridpack.make(gridpack_json)
        self.terminate_gridpack(gridpack)
        self.database.delete_gridpack(gridpack)
        gridpack.rmdir()

    def get_fragment(self, gridpack: Gridpack):
        """
        Retrieve the fragment for a Gridpack.
        In case the Gridpack reused the output from another
        its output file will be used to build the current
        fragment.

        Args:
            Gridpack: Gridpack to construct the McM fragment.

        Returns:
            tuple[str, bool]: Gridpack's fragment and a signal
                to determine if there is a valid file already set
                for this element.
        """
        fragment: str = ''
        valid_file: bool = False
        fragment = FragmentBuilder().build_fragment(gridpack=gridpack)
        valid_file = bool(gridpack.get('archive') and gridpack.get_absolute_path())
        return (fragment, valid_file)

    def terminate_gridpack(self, gridpack):
        """
        Terminate gridpack job in HTCondor
        """
        self.logger.info('Trying to terminate %s', gridpack)
        condor_id = gridpack.get_condor_id()
        if condor_id > 0:
            with HTCondorExecutor(
                SUBMISSION_HOST, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
            ) as ssh:
                ssh.execute_command(f'condor_rm {condor_id}')
        else:
            self.logger.info('Gridpack %s HTCondor id %s is not valid', gridpack, condor_id)

        self.logger.info('Finished terminating gridpack %s', gridpack)

    def submit_to_condor(self, gridpack):
        self.logger.info('Submitting %s', gridpack)
        gridpack.rmdir()
        gridpack.mkdir()

        try:
            self.logger.info('Will create files for %s', gridpack)
            # Prepare files
            gridpack.prepare_job_archive()
            gridpack.prepare_script()
            gridpack.prepare_jds_file()
            self.logger.info('Done preparing:\n%s',
                             os.popen('ls -l %s' % (gridpack.local_dir())).read())

            self.logger.info('Will prepare remote directory for %s', gridpack)
            # Prepare remote directory. Delete old one and create a new one
            gridpack_id = gridpack.get_id()
            remote_directory_base = REMOTE_DIRECTORY
            remote_directory = f'{remote_directory_base}/{gridpack_id}'
            with HTCondorExecutor(
                SUBMISSION_HOST, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
            ) as ssh:
                ssh.execute_command([f'rm -rf {remote_directory}',
                                     f'mkdir -p {remote_directory}'])

                self.logger.info('Will upload files for %s', gridpack)
                # Upload gridpack input_files.tar.gz, submit file and script to run
                local_directory = gridpack.local_dir()
                ssh.upload_file(f'{local_directory}/GRIDPACK_{gridpack_id}.sh',
                                f'{remote_directory}/GRIDPACK_{gridpack_id}.sh',)
                ssh.upload_file(f'{local_directory}/GRIDPACK_{gridpack_id}.jds',
                                f'{remote_directory}/GRIDPACK_{gridpack_id}.jds',)
                ssh.upload_file(f'{local_directory}/input_files.tar.gz',
                                f'{remote_directory}/input_files.tar.gz',)

                self.logger.info('Will try to submit %s', gridpack)
                # Run condor_submit
                # Submission happens through lxplus as condor is not available
                # on website machine
                # It is easier to ssh to lxplus than set up condor locally.
                submission_command = [
                    f'cd {remote_directory}',
                    f'condor_submit GRIDPACK_{gridpack_id}.jds'
                ]
                stdout, stderr, _ = ssh.execute_command(submission_command)

            self.logger.debug(stdout)
            self.logger.debug(stderr)
            # Parse result of condor_submit
            if stdout and '1 job(s) submitted to cluster' in stdout:
                # output is "1 job(s) submitted to cluster 801341"
                gridpack.set_status('submitted')
                condor_id = int(float(stdout.split()[-1]))
                gridpack.set_condor_id(condor_id)
                gridpack.set_condor_status('IDLE')
                self.logger.info('Submitted %s. Condor job id %s', gridpack, condor_id)
                gridpack.add_history_entry('submitted')
                # Send an email about submitted gridpack
                input_files = []
                # Attach the script file for debugging
                if os.path.isfile(f'{local_directory}/GRIDPACK_{gridpack_id}.sh'):
                    input_files.append(f'{local_directory}/GRIDPACK_{gridpack_id}.sh')

                # Attach the cards archive for debugging
                if os.path.isfile(f'{local_directory}/input_files.tar.gz'):
                    input_files.append(f'{local_directory}/input_files.tar.gz')

                attachments = []
                if input_files:
                    zip_file_name = f'{local_directory}/gridpack_{gridpack_id}_input_files.zip'
                    attachments.append(zip_file_name)
                    with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zip_object:
                        for file_path in input_files:
                            zip_object.write(file_path, file_path.split('/')[-1])

                self.send_submitted_notification(gridpack, attachments)
            else:
                self.logger.error('Error submitting %s.\nOutput: %s.\nError %s',
                                  gridpack,
                                  stdout,
                                  stderr)
                gridpack.set_status('failed')
                gridpack.add_history_entry('submission failed')

        except Exception as ex:
            gridpack.set_status('failed')
            gridpack.add_history_entry('submission failed')
            self.logger.error('Exception while trying to submit %s: %s\n%s',
                              gridpack,
                              str(ex),
                              traceback.format_exc())

        self.database.update_gridpack(gridpack)

    def update_condor_status(self, gridpack, condor_jobs):
        """
        Update condor status for given gridpack
        """
        condor_id = str(gridpack.get_condor_id())
        condor_status = condor_jobs.get(condor_id, 'REMOVED')
        self.logger.info('Saving %s condor status as %s', gridpack, condor_status)
        if condor_status != gridpack.get_condor_status():
            gridpack.add_history_entry(f'job {condor_status}')

        gridpack.set_condor_status(condor_status)
        self.database.update_gridpack(gridpack)

    def collect_output(self, gridpack: Gridpack):
        """
        When gridpack finishes running in HTCondor, download it's output logs,
        zip them and send to relevant user via email
        """
        condor_status = gridpack.get_condor_status()
        if condor_status not in ['DONE', 'REMOVED']:
            self.logger.info('%s status is not DONE or REMOVED, it is %s', gridpack, condor_status)
            return

        self.logger.info('Collecting output for %s', gridpack)
        remote_directory_base = REMOTE_DIRECTORY
        gridpack_id = gridpack.get_id()
        dataset_name = gridpack.data['dataset']
        remote_directory = f'{remote_directory_base}/{gridpack_id}'
        submission_host = SUBMISSION_HOST
        local_directory = gridpack.local_dir()

        stdout = ''
        gridpack_archive = ''
        with HTCondorExecutor(
            submission_host, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
        ) as ssh:
            ssh.download_file(f'{remote_directory}/job.log',
                              f'{local_directory}/job.log')
            ssh.download_file(f'{remote_directory}/output.log',
                              f'{local_directory}/output.log')
            ssh.download_file(f'{remote_directory}/error.log',
                              f'{local_directory}/error.log')
            # Get gridpack archive name
            stdout, stderr, _ = ssh.execute_command([f'ls -1 {remote_directory}/*{dataset_name}*.t*z'])
            self.logger.debug(stdout)
            self.logger.debug(stderr)
            stdout = clean_split(stdout, '\n')
            for line in stdout:
                filename = clean_split(line, '/')[-1]
                if dataset_name in filename and filename.endswith(('.tar.xz', '.tar.gz', '.tgz')):
                    gridpack_archive = filename
                    break

            if gridpack_archive:
                gridpack_directory = gridpack.get_remote_storage_path()
                self.logger.info('Copying gridpack %s/%s->%s', remote_directory, gridpack_archive, gridpack_directory)
                sync_command: str = (
                    'rsync -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" '
                    f'{remote_directory}/{gridpack_archive} '
                    f'{submission_host}:{gridpack_directory}'
                )
                self.logger.info('Sync command: %s', sync_command)
                stdout, stderr, _ = ssh.execute_command(sync_command)
                self.logger.debug(stdout)
                self.logger.debug(stderr)

            # Remove the directory
            ssh.execute_command([f'rm -rf {remote_directory}'])

        downloaded_files = []
        if os.path.isfile(f'{local_directory}/job.log'):
            downloaded_files.append(f'{local_directory}/job.log')

        if os.path.isfile(f'{local_directory}/output.log'):
            downloaded_files.append(f'{local_directory}/output.log')

        if os.path.isfile(f'{local_directory}/error.log'):
            downloaded_files.append(f'{local_directory}/error.log')

        # Attach the script file for debugging
        if os.path.isfile(f'{local_directory}/GRIDPACK_{gridpack_id}.sh'):
            downloaded_files.append(f'{local_directory}/GRIDPACK_{gridpack_id}.sh')

        # Attach the cards archive for debugging
        if os.path.isfile(f'{local_directory}/input_files.tar.gz'):
            downloaded_files.append(f'{local_directory}/input_files.tar.gz')

        attachments = []
        if downloaded_files:
            zip_file_name = f'{local_directory}/gridpack_{gridpack_id}_files.zip'
            attachments.append(zip_file_name)
            with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zip_object:
                for file_path in downloaded_files:
                    zip_object.write(file_path, file_path.split('/')[-1])

        gridpack.data['archive'] = gridpack_archive
        if gridpack.get_status() != 'failed':
            gridpack.set_status('done')
            gridpack.add_history_entry('done')
            self.send_done_notification(gridpack, files=attachments)
        else:
            gridpack.add_history_entry('failed')
            self.send_failed_notification(gridpack, files=attachments)

        gridpack.rmdir()
        self.database.update_gridpack(gridpack)
        self.gridpacks_to_create_requests.append(gridpack_id)

    def create_mcm_request(self, gridpack):
        """
        Create a request in McM for the given gridpack
        """
        remote_directory_base = TICKETS_DIRECTORY
        gridpack_id = gridpack.get_id()
        remote_directory = f'{remote_directory_base}/{gridpack_id}'
        fragment, valid_file = self.get_fragment(gridpack)
        chain = gridpack.get_campaign_dict().get('chain')
        dataset_name = gridpack.get('dataset_name')
        events = gridpack.get('events')
        process = gridpack.get('process')
        generator = gridpack.get('generator')

        if not valid_file:
            gridpack.set_status('failed')
            gridpack.add_history_entry('invalid gridpack file')
            self.send_invalid_mcm_request_notification(gridpack=gridpack)
            return

        with SSHExecutor(
            SUBMISSION_HOST, SERVICE_ACCOUNT_USERNAME, SERVICE_ACCOUNT_PASSWORD
        ) as ssh:
            ssh.execute_command([f'rm -rf {remote_directory}',
                                 f'mkdir -p {remote_directory}'])
            ssh.upload_file('mcm_gridpack.py',
                            f'{remote_directory}/mcm_gridpack.py')
            ssh.upload_as_file(fragment,
                              f'{remote_directory}/fragment.py')

            dev = not PRODUCTION
            command = [f'cd {remote_directory}',
                       f'python3 mcm_gridpack.py {"--dev" if dev else ""} '
                       '--fragment "fragment.py" '
                       f'--chain "{chain}" '
                       f'--dataset "{dataset_name}" '
                       f'--events "{events}" '
                       f'--tag "{process}" '
                       f'--generator "{generator}"'
                       ]
            stdout, stderr, _ = ssh.execute_command(command)
            self.logger.debug(stdout)
            self.logger.debug(stderr)
            stdout = clean_split(stdout, '\n')
            prepid = None
            for line in stdout:
                if line.startswith('REQUEST PREPID:'):
                    prepid = line.replace('REQUEST PREPID:', '').strip()
                    break

            gridpack.set_prepid(prepid)
            # Remove the directory
            ssh.execute_command([f'rm -rf {remote_directory}'])

    def send_submitted_notification(self, gridpack, files=None):
        """
        Send email notification that gridpack was submitted
        """
        gridpack_dict = gridpack.get_json()
        campaign = gridpack_dict.get('campaign')
        generator = gridpack_dict.get('generator')
        dataset = gridpack_dict.get('dataset')
        gridpack_id = gridpack.get_id()
        gridpack_name = f'{campaign} {dataset} {generator}'
        service_url = SERVICE_URL
        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) job was submitted.\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} was submitted'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(
            SERVICE_ACCOUNT_USERNAME, 
            SERVICE_ACCOUNT_PASSWORD,
            EMAIL_AUTH,
            PRODUCTION
        )
        emailer.send(subject, body, recipients, files)

    def send_done_notification(self, gridpack, files=None):
        """
        Send email notification that gridpack has successfully finished
        """
        gridpack_dict = gridpack.get_json()
        campaign = gridpack_dict.get('campaign')
        generator = gridpack_dict.get('generator')
        dataset = gridpack_dict.get('dataset')
        gridpack_id = gridpack.get_id()
        gridpack_name = f'{campaign} {dataset} {generator}'
        service_url = SERVICE_URL
        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) job has finished running.\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} is done'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(
            SERVICE_ACCOUNT_USERNAME, 
            SERVICE_ACCOUNT_PASSWORD,
            EMAIL_AUTH,
            PRODUCTION
        )
        emailer.send(subject, body, recipients, files)

    def send_reused_notification(self, gridpack, files=None):
        """
        Send an email notification that this gridpack will reuse 
        an available one to avoid creating a new one via batch jobs.
        """
        gridpack_dict = gridpack.get_json()
        campaign = gridpack_dict.get('campaign')
        generator = gridpack_dict.get('generator')
        dataset = gridpack_dict.get('dataset')
        gridpack_id = gridpack.get_id()
        gridpack_name = f'{campaign} {dataset} {generator}'
        service_url = SERVICE_URL

        # Retrieve the Gridpack file path.
        gridpack_ref: str = ''
        gridpack_path: str = ''
        try:
            gridpack_path = gridpack.get_absolute_path()
            if not gridpack_path:
                gridpack_reused = self.get_original_gridpack(gridpack.get_id())
                gridpack_path = gridpack_reused.get_absolute_path()
            gridpack_ref = f'Gridpack reused: {gridpack_path}\n'
        except AssertionError:
            gridpack_ref = (
                'Unable to link the reused Gridpack file with '
                'the Gridpack request that created it.\n'
                'Maybe, this file being reused was created manually and then moved '
                'to the storage folder to reused it as input for more Gridpack '
                'requests in this application.\n'
            )

        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) will reuse artifacts.\n'
        body += (
            'Instead of creating a new Gridpack via a batch job. '
            'This Gridpack used one that already existed\n'
        )
        body += gridpack_ref
        body += f'A request in McM will be created\n'
        body += f'For more details, please see\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} is reusing artifacts from another Gridpack'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(
            SERVICE_ACCOUNT_USERNAME, 
            SERVICE_ACCOUNT_PASSWORD,
            EMAIL_AUTH,
            PRODUCTION
        )
        emailer.send(subject, body, recipients, files)

    def send_failed_reused_notification(
            self, 
            gridpack: Gridpack, 
            cause: str
        ):
        """
        Send an email notification that this gridpack could not 
        reuse an available one to avoid creating a new one via batch jobs
        because of errors.
        """
        gridpack_dict = gridpack.get_json()
        campaign = gridpack_dict.get('campaign')
        generator = gridpack_dict.get('generator')
        dataset = gridpack_dict.get('dataset')
        gridpack_id = gridpack.get_id()
        gridpack_name = f'{campaign} {dataset} {generator}'
        service_url = SERVICE_URL

        body = 'Hello,\n\n'
        body += (
            f'Gridpack {gridpack_name} ({gridpack_id}) '
            'could not reuse output artifacts from old Gridpacks and therefore it failed.\n'
        )
        body += f'{cause}\n'
        body += f'For more details, please see\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'

        subject = f'Gridpack {gridpack_name} failed to reuse artifacts from another Gridpack'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(
            SERVICE_ACCOUNT_USERNAME, 
            SERVICE_ACCOUNT_PASSWORD,
            EMAIL_AUTH,
            PRODUCTION
        )
        emailer.send(subject, body, recipients)

    def send_invalid_mcm_request_notification(self, gridpack):
        """
        Send email notification that gridpack doesn't have a valid output file
        to create a request in McM.
        """
        gridpack_dict = gridpack.get_json()
        campaign = gridpack_dict.get('campaign')
        generator = gridpack_dict.get('generator')
        dataset = gridpack_dict.get('dataset')
        gridpack_id = gridpack.get_id()
        gridpack_name = f'{campaign} {dataset} {generator}'
        service_url = SERVICE_URL
        body = 'Hello,\n\n'
        body += (
            f'Gridpack {gridpack_name} ({gridpack_id}) does not have a valid output file '
            'to include in the McM request fragment. Therefore, no McM request is going to be created.\n'
        )
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        subject = f'Gridpack {gridpack_name} failed to retrieve the output file to create a McM request'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(
            SERVICE_ACCOUNT_USERNAME, 
            SERVICE_ACCOUNT_PASSWORD,
            EMAIL_AUTH,
            PRODUCTION
        )
        emailer.send(subject, body, recipients)


    def send_failed_notification(self, gridpack, files=None):
        """
        Send email notification that gridpack has failed
        """
        gridpack_dict = gridpack.get_json()
        campaign = gridpack_dict.get('campaign')
        generator = gridpack_dict.get('generator')
        dataset = gridpack_dict.get('dataset')
        gridpack_id = gridpack.get_id()
        gridpack_name = f'{campaign} {dataset} {generator}'
        service_url = SERVICE_URL
        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) job has failed.\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} job failed'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(
            SERVICE_ACCOUNT_USERNAME, 
            SERVICE_ACCOUNT_PASSWORD,
            EMAIL_AUTH,
            PRODUCTION
        )
        emailer.send(subject, body, recipients, files)
