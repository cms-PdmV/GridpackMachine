import json
import logging
import subprocess
from connection_wrapper import ConnectionWrapper


CONDOR_STATUS = {'0': 'UNEXPLAINED',
                 '1': 'IDLE',
                 '2': 'RUN',
                 '3': 'REMOVED',
                 '4': 'DONE',
                 '5': 'HOLD',
                 '6': 'SUBMISSION ERROR'}


TAGS_CACHE = {}


def clean_split(string, separator=',', maxsplit=-1):
    """
    Split a string by separator and collect only non-empty values
    """
    return [x.strip() for x in string.split(separator, maxsplit) if x.strip()]


def run_command(command):
    """
    Run a bash command and return stdout, stderr and exit code
    """
    if isinstance(command, list):
        command = '\n'.join(command)

    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    code = process.returncode
    stdout = stdout.decode('utf-8') if stdout is not None else None
    stderr = stderr.decode('utf-8') if stderr is not None else None
    return stdout, stderr, code


def get_jobs_in_condor(ssh=None):
    """
    Fetch jobs from HTCondor
    Return a dictionary where key is job id and value is status (IDLE, RUN, ...)
    """
    cmd = 'condor_q -af:h ClusterId JobStatus Cmd'
    if ssh:
        stdout, stderr, exit_code = ssh.execute_command(cmd)
    else:
        stdout, stderr, exit_code = run_command(cmd)

    if exit_code != 0:
        logger.error('HTCondor is failing (%s):\n%s\n%s', exit_code, stdout, stderr)
        raise Exception('HTCondor status check returned %s', exit_code)

    logger = logging.getLogger()
    lines = stdout.split('\n')
    if not lines or 'ClusterId JobStatus Cmd' not in lines[0]:
        logger.error('HTCondor is failing (%s):\n%s\n%s', exit_code, stdout, stderr)
        raise Exception('HTCondor is not working')

    jobs_dict = {}
    lines = lines[1:]
    for line in lines:
        if 'GRIDPACK_' not in line:
            continue

        columns = line.split()
        if not columns:
            continue

        job_id = columns[0]
        job_status = columns[1]
        jobs_dict[job_id] = CONDOR_STATUS.get(job_status, 'REMOVED')

    logger.info('Job status in HTCondor: %s', json.dumps(jobs_dict))
    return jobs_dict


def get_git_tags(repository, cache=True):
    """
    Return list of tags in the repostory
    """
    if not cache or repository not in TAGS_CACHE:
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'}
        with ConnectionWrapper('https://api.github.com') as conn:
            response = conn.api('GET', f'/repos/{repository}/git/refs/tags', headers=headers)

        response = json.loads(response.decode('utf-8'))
        logger = logging.getLogger()
        tags = [t['ref'].replace('refs/tags/', '') for t in response if t['ref'].startswith('refs/tags/')]
        logger.debug('Found %s tags in %s', len(tags), repository)
        TAGS_CACHE[repository] = tags

    return TAGS_CACHE[repository]
