"""
Module that handles all SSH operations - both ssh and ftp
and operations related to HTCondor involving the execution of remote
commands over SSH.
"""
import json
import time
import logging
from io import BytesIO
from typing import Union
import paramiko
from config import Config


class SSHExecutor():
    """
    SSH executor allows to perform remote commands and upload/download files
    """

    def __init__(self, host, credentials_path):
        self.ssh_client = None
        self.ftp_client = None
        self.logger = logging.getLogger()
        self.remote_host = host
        self.credentials_file_path = credentials_path
        self.timeout = 3600
        self.max_retries = 3

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close_connections()
        return False

    def setup_ssh(self):
        """
        Initiate SSH connection and save it as self.ssh_client
        """
        self.logger.debug('Will set up ssh')
        if self.ssh_client:
            self.close_connections()

        with open(self.credentials_file_path) as json_file:
            credentials = json.load(json_file)

        self.logger.info('Credentials loaded successfully: %s', credentials['username'])
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(self.remote_host,
                                username=credentials["username"],
                                password=credentials["password"],
                                timeout=30)
        self.logger.debug('Done setting up ssh')

    def setup_ftp(self):
        """
        Initiate SFTP connection and save it as self.ftp_client
        If needed, SSH connection will be automatically set up
        """
        self.logger.debug('Will set up ftp')
        if self.ftp_client:
            self.close_connections()

        if not self.ssh_client:
            self.setup_ssh()

        self.ftp_client = self.ssh_client.open_sftp()
        self.logger.debug('Done setting up ftp')

    def execute_command(self, command):
        """
        Execute command over SSH
        """
        start_time = time.time()
        if isinstance(command, list):
            command = '; '.join(command)

        self.logger.debug('Executing %s', command)
        retries = 0
        while retries <= self.max_retries:
            if not self.ssh_client:
                self.setup_ssh()

            (_, stdout, stderr) = self.ssh_client.exec_command(command, timeout=self.timeout)
            self.logger.debug('Executed %s. Reading response', command)
            stdout_list = []
            stderr_list = []
            for line in stdout.readlines():
                stdout_list.append(line[0:256])

            for line in stderr.readlines():
                stderr_list.append(line[0:256])

            exit_code = stdout.channel.recv_exit_status()
            stdout = ''.join(stdout_list).strip()
            stderr = ''.join(stderr_list).strip()
            # Retry if AFS error occured
            if '.bashrc: Permission denied' in stderr:
                retries += 1
                self.logger.warning('SSH execution failed, will do a retry number %s', retries)
                self.close_connections()
                time.sleep(3)
            else:
                break

        end_time = time.time()
        # Read output from stdout and stderr streams
        self.logger.info('SSH command exit code %s, executed in %.2fs, command:\n\n%s\n',
                         exit_code,
                         end_time - start_time,
                         command.replace('; ', '\n'))

        if stdout:
            self.logger.debug('STDOUT: %s', stdout)

        if stderr:
            self.logger.error('STDERR: %s', stderr)

        return stdout, stderr, exit_code

    def upload_as_file(self, content, copy_to):
        """
        Upload given string as file
        """
        self.logger.debug('Will upload %s bytes as %s', len(content), copy_to)
        if not self.ftp_client:
            self.setup_ftp()

        try:
            self.ftp_client.putfo(BytesIO(content.encode()), copy_to)
            self.logger.debug('Uploaded string to %s', copy_to)
        except Exception as ex:
            self.logger.error('Error uploading file to %s. %s', copy_to, ex)
            return False

        return True

    def upload_file(self, copy_from, copy_to):
        """
        Upload a file
        """
        self.logger.debug('Will upload file %s to %s', copy_from, copy_to)
        if not self.ftp_client:
            self.setup_ftp()

        try:
            self.ftp_client.put(copy_from, copy_to)
            self.logger.debug('Uploaded file to %s', copy_to)
        except Exception as ex:
            self.logger.error('Error uploading file from %s to %s. %s', copy_from, copy_to, ex)
            return False

        return True

    def download_as_string(self, copy_from):
        """
        Download remote file contents as string
        """
        self.logger.debug('Will download file %s as string', copy_from)
        if not self.ftp_client:
            self.setup_ftp()

        remote_file = None
        try:
            remote_file = self.ftp_client.open(copy_from)
            contents = remote_file.read()
            self.logger.debug('Downloaded %s bytes from %s', len(contents), copy_from)
            return contents.decode('utf-8')
        except Exception as ex:
            self.logger.error('Error downloading file from %s. %s', copy_from, ex)
        finally:
            if remote_file:
                remote_file.close()

        return None

    def download_file(self, copy_from, copy_to):
        """
        Download file from remote host
        """
        self.logger.debug('Will download file %s to %s', copy_from, copy_to)
        if not self.ftp_client:
            self.setup_ftp()

        try:
            self.ftp_client.get(copy_from, copy_to)
            self.logger.debug('Downloaded file to %s', copy_to)
        except Exception as ex:
            self.logger.error('Error downloading file from %s to %s. %s', copy_from, copy_to, ex)
            return False

        return True

    def close_connections(self):
        """
        Close any active connections
        """
        if self.ftp_client:
            self.logger.debug('Closing ftp client')
            self.ftp_client.close()
            self.ftp_client = None
            self.logger.debug('Closed ftp client')

        if self.ssh_client:
            self.logger.debug('Closing ssh client')
            self.ssh_client.close()
            self.ssh_client = None
            self.logger.debug('Closed ssh client')


class HTCondorExecutor(SSHExecutor):
    """
    SSHExecutor that sets required environments
    for interacting with some special pool of HTCondor nodes
    like CMS CAF.
    """

    ENABLE_CMS_CAF_ENV = 'module load lxbatch/tzero'
    CMS_CAF_GROUP = 'group_u_CMS.CAF.PHYS'
    LXBATCH_PRIORITY_GROUP = 'group_u_CMS.u_zh.priority'

    def __set_env(self) -> str:
        """
        Retrieves the instruction(s) required to set the desired
        HTCondor environment.

        Returns:
            str: Extra instructions to set the desired environment,
                a blank string if returned if it is not required to choose
                an special environment.
        """
        to_caf: bool = bool(Config.get('use_htcondor_cms_caf'))
        if to_caf:
            self.logger.info('HTCondor nodes: Running command to CMS CAF')
            return self.ENABLE_CMS_CAF_ENV
        
        return ''


    @staticmethod
    def retrieve_accounting_group() -> str:
        """
        Retrieves the AccountingGroup attribute to use in
        HTCondor configuration files.
        """
        to_caf: bool = bool(Config.get('use_htcondor_cms_caf'))
        if to_caf:
            return HTCondorExecutor.CMS_CAF_GROUP
        
        return HTCondorExecutor.LXBATCH_PRIORITY_GROUP


    def execute_command(self, command):
        """
        Execute command over SSH related to HTCondor operations

        Args:
            command (str | list[str]): Command(s) to execute
        """
        enable_env: str = self.__set_env()
        command_and_env = ""
        if not isinstance(command, list) and not isinstance(command, str):
            msg = (
                "Invalid type for the command - "
                "Expected list[str] or str - "
                f"Received: {type(command)}"
            )
            raise ValueError(msg)
        
        if not enable_env:
            return super(HTCondorExecutor, self).execute_command(command=command)

        if isinstance(command, list):
            command_and_env = command.copy()
            command_and_env.insert(0, enable_env)
            return super(HTCondorExecutor, self).execute_command(command=command_and_env)
    
        # Complete the string command
        command_and_env = '; '.join([enable_env, command])
        return super(HTCondorExecutor, self).execute_command(command=command_and_env)
