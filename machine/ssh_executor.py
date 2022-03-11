"""
Module that handles all SSH operations - both ssh and ftp
"""
import json
import logging
import time
import paramiko


class SSHExecutor():
    """
    SSH executor allows to perform remote commands and upload/download files
    """

    def __init__(self, config):
        self.ssh_client = None
        self.ftp_client = None
        self.logger = logging.getLogger('logger')
        self.remote_host = config['submission_host']
        self.credentials = config['ssh_credentials']

    def setup_ssh(self):
        """
        Initiate SSH connection and save it as self.ssh_client
        """
        self.logger.info('Will set up ssh')
        if self.ssh_client:
            self.close_connections()

        if ':' not in self.credentials:
            with open(self.credentials) as json_file:
                credentials = json.load(json_file)
        else:
            credentials = {}
            credentials['username'] = self.credentials.split(':')[0]
            credentials['password'] = self.credentials.split(':')[1]

        self.logger.info('Credentials loaded successfully: %s', credentials['username'])
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(self.remote_host,
                                username=credentials['username'],
                                password=credentials['password'],
                                timeout=30)
        self.logger.info('Done setting up ssh')

    def setup_ftp(self):
        """
        Initiate SFTP connection and save it as self.ftp_client
        If needed, SSH connection will be automatically set up
        """
        self.logger.info('Will set up ftp')
        if self.ftp_client:
            self.close_connections()

        if not self.ssh_client:
            self.setup_ssh()

        self.ftp_client = self.ssh_client.open_sftp()
        self.logger.info('Done setting up ftp')

    def execute_command(self, command):
        """
        Execute command over SSH
        """
        if not self.ssh_client:
            self.setup_ssh()

        if isinstance(command, list):
            command = '; '.join(command)

        self.logger.info('Executing %s', command)
        (_, stdout, stderr) = self.ssh_client.exec_command(command)
        self.logger.info('Executed %s. Reading response', command)
        # Close channel after minute of waiting for EOF
        # This timeouts and closes channel if nothing was received
        stdout_timeout = time.time() + 60
        while not stdout.channel.eof_received:
            time.sleep(1)
            if time.time() > stdout_timeout:
                stdout.channel.close()
                break

        stdout = stdout.read().decode('utf-8').strip()
        # Same thing for stderr
        stderr_timeout = time.time() + 60
        while not stderr.channel.eof_received:
            time.sleep(1)
            if time.time() > stderr_timeout:
                stderr.channel.close()
                break

        stderr = stderr.read().decode('utf-8').strip()
        # Read output from stdout and stderr streams
        if stdout:
            self.logger.info('STDOUT (%s): %s', command, stdout)

        if stderr:
            self.logger.error('STDERR (%s): %s', command, stderr)

        return stdout, stderr

    def upload_file(self, copy_from, copy_to):
        """
        Upload a file
        """
        self.logger.info('Will upload file %s to %s', copy_from, copy_to)
        if not self.ftp_client:
            self.setup_ftp()

        try:
            self.ftp_client.put(copy_from, copy_to)
            self.logger.info('Uploaded file to %s', copy_to)
        except Exception as ex:
            self.logger.error('Error uploading file from %s to %s. %s', copy_from, copy_to, ex)

    def download_file(self, copy_from, copy_to):
        """
        Download file from remote host
        """
        self.logger.info('Will download file %s to %s', copy_from, copy_to)
        if not self.ftp_client:
            self.setup_ftp()

        try:
            self.ftp_client.get(copy_from, copy_to)
            self.logger.info('Downloaded file to %s', copy_to)
        except Exception as ex:
            self.logger.error('Error downloading file from %s to %s. %s', copy_from, copy_to, ex)

    def close_connections(self):
        """
        Close any active connections
        """
        if self.ftp_client:
            self.logger.info('Closing ftp client')
            self.ftp_client.close()
            self.ftp_client = None
            self.logger.info('Closed ftp client')

        if self.ssh_client:
            self.logger.info('Closing ssh client')
            self.ssh_client.close()
            self.ssh_client = None
            self.logger.info('Closed ssh client')
