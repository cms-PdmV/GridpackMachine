import time
import logging
import os
import zipfile
import traceback
from database import Database
from gridpack import Gridpack
from email_sender import EmailSender
from utils import (clean_split,
                   get_available_campaigns,
                   get_available_cards,
                   get_git_branches,
                   get_available_tunes,
                   get_jobs_in_condor,
                   run_command)
from ssh_executor import SSHExecutor
from config import Config
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
        self.gridpacks_to_delete = []
        self.gridpacks_to_create_requests = []
        self.repository_tick_pause = 60
        self.tick_lock = Lock()

    def update_repository_tree(self):
        now = int(time.time())
        if now - self.repository_tick_pause < self.last_repository_tick:
            self.logger.info('Not updating repository, last update happened recently')
            return

        branches = get_git_branches(Config.get('gen_repository'), cache=False)
        branches = branches[::-1]
        files_dir = Config.get('gridpack_files_path')
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
            submission_host = Config.get('submission_host')
            ssh_credentials = Config.get('ssh_credentials')
            with SSHExecutor(submission_host, ssh_credentials) as ssh:
                condor_jobs = get_jobs_in_condor(ssh)

        for gridpack_json in gridpacks_to_check:
            gridpack = Gridpack.make(gridpack_json)
            self.update_condor_status(gridpack, condor_jobs)
            condor_status = gridpack.get_condor_status()
            if condor_status in ('DONE', 'REMOVED'):
                # Refetch after check if running save
                self.collect_output(gridpack)

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
        self.logger.info('Adding %s to approve list', gridpack_id)
        self.gridpacks_to_approve.append(gridpack_id)

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
        self.create_mcm_request(gridpack)
        gridpack.add_history_entry('create request')
        self.database.update_gridpack(gridpack)

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

    def terminate_gridpack(self, gridpack):
        """
        Terminate gridpack job in HTCondor
        """
        self.logger.info('Trying to terminate %s', gridpack)
        condor_id = gridpack.get_condor_id()
        if condor_id > 0:
            submission_host = Config.get('submission_host')
            ssh_credentials = Config.get('ssh_credentials')
            with SSHExecutor(submission_host, ssh_credentials) as ssh:
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
            remote_directory_base = Config.get('remote_directory')
            submission_host = Config.get('submission_host')
            ssh_credentials = Config.get('ssh_credentials')

            # Append the Generator/Process to the submission gridpack directory path
            gridpack_generator = gridpack.get('generator')
            gridpack_process = gridpack.get('process')
            self.logger.info(
                '[submit_to_condor] Submission to condor: Gridpack directory base path: %s -> Appending GENERATOR/PROCESS: %s/%s',
                remote_directory_base, gridpack_generator, gridpack_process
            )
            remote_directory = "/".join([remote_directory_base, gridpack_generator, gridpack_process, gridpack_id])
            self.logger.info('[submit_to_condor] New gridpack directory path: %s', remote_directory)
            with SSHExecutor(submission_host, ssh_credentials) as ssh:
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
                # It is easier to ssh to lxplus than set up condor locally
                stdout, stderr, _ = ssh.execute_command([f'cd {remote_directory}',
                                                         f'condor_submit GRIDPACK_{gridpack_id}.jds'])

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

    def collect_output(self, gridpack):
        """
        When gridpack finishes running in HTCondor, download it's output logs,
        zip them and send to relevant user via email
        """
        condor_status = gridpack.get_condor_status()
        if condor_status not in ['DONE', 'REMOVED']:
            self.logger.info('%s status is not DONE or REMOVED, it is %s', gridpack, condor_status)
            return

        self.logger.info('Collecting output for %s', gridpack)
        remote_directory_base = Config.get('remote_directory')
        gridpack_id = gridpack.get_id()
        dataset_name = gridpack.data['dataset']

        # Remote directory used for submission
        # Append the Generator/Process to the submission gridpack directory path
        gridpack_generator = gridpack.get('generator')
        gridpack_process = gridpack.get('process')
        remote_directory = "/".join([remote_directory_base, gridpack_generator, gridpack_process, gridpack_id])
        self.logger.info(
            '[collect_output] Retriving Gridpack execution logs from: %s',
            remote_directory
        )
        submission_host = Config.get('submission_host')
        ssh_credentials = Config.get('ssh_credentials')
        local_directory = gridpack.local_dir()

        stdout = ''
        gridpack_archive = ''
        with SSHExecutor(submission_host, ssh_credentials) as ssh:
            # Get gridpack archive name
            stdout, stderr, _ = ssh.execute_command([f'ls -1 {remote_directory}/*{dataset_name}*.t*z'])
            self.logger.debug(stderr)
            self.logger.debug(stdout)

            stdout = clean_split(stdout, '\n')
            for line in stdout:
                filename = clean_split(line, '/')[-1]
                if dataset_name in filename and filename.endswith(('.tar.xz', '.tar.gz', '.tgz')):
                    gridpack_archive = filename
                    break

            # If gridpack_archive path is empty, maybe we are dealing with an update before restructure
            # of the path storage. Use the old remote storage pattern
            if not gridpack_archive:
                remote_directory = "/".join([remote_directory_base, gridpack_id])
                self.logger.info(
                    '[collect_output] Checking if Gridpack was stored inside a folder that uses the old submission path: %s',
                    remote_directory
                )
                # Get gridpack archive name
                stdout, stderr, _ = ssh.execute_command([f'ls -1 {remote_directory}/*{dataset_name}*.t*z'])
                self.logger.debug(stderr)
                stdout = clean_split(stdout, '\n')
                for line in stdout:
                    filename = clean_split(line, '/')[-1]
                    if dataset_name in filename and filename.endswith(('.tar.xz', '.tar.gz', '.tgz')):
                        gridpack_archive = filename
                        break

            if gridpack_archive:
                self.logger.info(
                    '[collect_output] Retriving execution output from: %s',
                    remote_directory
                )

                # Download content
                ssh.download_file(f'{remote_directory}/job.log',
                                  f'{local_directory}/job.log')
                ssh.download_file(f'{remote_directory}/output.log',
                                  f'{local_directory}/output.log')
                ssh.download_file(f'{remote_directory}/error.log',
                                  f'{local_directory}/error.log')

                gridpack_directory = Config.get('gridpack_directory')
                if not Config.get('dev'):
                    campaign_dict = gridpack.get_campaign_dict()
                    gridpack_directory = campaign_dict.get('gridpack_directory', gridpack_directory)

                # Append the Generator/Process to the submission gridpack directory path
                gridpack_generator = gridpack.get('generator')
                gridpack_process = gridpack.get('process')
                self.logger.info(
                    'Gridpack directory base path: %s -> Appending GENERATOR/PROCESS: %s/%s',
                    gridpack_directory, gridpack_generator, gridpack_process
                )
                gridpack_directory = "/".join([gridpack_directory, gridpack_generator, gridpack_process])
                self.logger.info('New gridpack directory path: %s', gridpack_directory)
                self.logger.info('Copying gridpack %s/%s->%s', remote_directory, gridpack_archive, gridpack_directory)

                # Open another connection to lxplus.cern.ch to create the gridpack remote directory,
                # Sadly, rsync -avR seems to be unable to perform mkdir -p
                with SSHExecutor('lxplus.cern.ch', ssh_credentials) as lxplus_ssh:
                    # Get gridpack archive name
                    lxplus_stdout, lxplus_stderr, _ = lxplus_ssh.execute_command([f'mkdir -p {gridpack_directory}'])
                    self.logger.info(
                        '[collect_output] Creating remote storage path on eos through lxplus: %s' % gridpack_directory
                    )
                    self.logger.info('[collect_output] Standard error pipe:')
                    self.logger.info(lxplus_stderr)
                    self.logger.debug(lxplus_stdout)
                    self.logger.info('[collect_output] Closing connection to lxplus.cern.ch')

                stdout, stderr, _ = ssh.execute_command(f'rsync -av -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" {remote_directory}/{gridpack_archive} lxplus.cern.ch:{gridpack_directory}')
                self.logger.debug(stdout)
                self.logger.debug(stderr)

            # Remove the directory
            # ssh.execute_command([f'rm -rf {remote_directory}'])

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
        remote_directory_base = Config.get('tickets_directory')
        gridpack_id = gridpack.get_id()
        remote_directory = f'{remote_directory_base}/{gridpack_id}'
        tickets_host = Config.get('tickets_host')
        ssh_credentials = Config.get('ssh_credentials')
        fragment = FragmentBuilder().build_fragment(gridpack)
        chain = gridpack.get_campaign_dict().get('chain')
        dataset_name = gridpack.get('dataset_name')
        events = gridpack.get('events')
        with SSHExecutor(tickets_host, ssh_credentials) as ssh:
            ssh.execute_command([f'rm -rf {remote_directory}',
                                 f'mkdir -p {remote_directory}'])
            ssh.upload_file('mcm_gridpack.py',
                            f'{remote_directory}/mcm_gridpack.py')
            ssh.upload_as_file(fragment,
                              f'{remote_directory}/fragment.py')

            dev = Config.get('dev')
            command = [f'cd {remote_directory}',
                       f'python3 mcm_gridpack.py {"--dev" if dev else ""} '
                       '--fragment "fragment.py" '
                       f'--chain "{chain}" '
                       f'--dataset "{dataset_name}" '
                       f'--events "{events}"']
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
        service_url = Config.get('service_url')
        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) job was submitted.\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} was submitted'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(Config.get('ssh_credentials'))
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
        service_url = Config.get('service_url')
        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) job has finished running.\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} is done'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(Config.get('ssh_credentials'))
        emailer.send(subject, body, recipients, files)

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
        service_url = Config.get('service_url')
        body = 'Hello,\n\n'
        body += f'Gridpack {gridpack_name} ({gridpack_id}) job has failed.\n'
        body += f'Gridpack job: {service_url}?_id={gridpack_id}\n'
        if files:
            body += 'You can find job files as an attachment.\n'

        subject = f'Gridpack {gridpack_name} job failed'
        recipients = [f'{user}@cern.ch' for user in gridpack.get_users()]
        emailer = EmailSender(Config.get('ssh_credentials'))
        emailer.send(subject, body, recipients, files)
