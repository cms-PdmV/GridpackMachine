import json
import logging
import os
import os.path
import subprocess
import sys
import time


logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s',
                    level=logging.DEBUG if '--debug' in sys.argv else logging.INFO)
logger = logging.getLogger()

CARDS_REPO_DIR = 'GridpackExtravaganza'
CONDOR_STATUS = {'0': 'UNEXPLAINED',
                 '1': 'IDLE',
                 '2': 'RUN',
                 '3': 'REMOVED',
                 '4': 'DONE',
                 '5': 'HOLD',
                 '6': 'SUBMISSION ERROR'}

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


def get_jobs_in_condor():
    """
    Fetch jobs from HTCondor
    Return a dictionary where key is job id and value is status (IDLE, RUN, DONE)
    """
    cmd = 'condor_q -af:h ClusterId JobStatus'
    stdout, stderr, _ = run_command(cmd)
    lines = stdout.split('\n')
    if not lines or 'ClusterId JobStatus' not in lines[0]:
        logger.error('HTCondor is failing:\n%s\n%s', stdout, stderr)
        raise Exception('HTCondor is not working')

    jobs_dict = {}
    lines = lines[1:]
    for line in lines:
        columns = line.split()
        if not columns:
            continue

        job_id = columns[0]
        job_status = columns[1]
        jobs_dict[job_id] = CONDOR_STATUS.get(job_status, 'REMOVED')

    logger.info('Job status in HTCondor:%s', json.dumps(jobs_dict))
    return jobs_dict


def get_new_cards():
    """
    Get list of changed .dat files in /cards/ directories
    """
    command = [f'cd {CARDS_REPO_DIR}',
               'old_commit=$(git rev-parse HEAD)',
               'git pull --quiet',
               'new_commit=$(git rev-parse HEAD)',
               # Get only files that are in /cards/ directory and end with .dat
               'git diff --name-only $old_commit $new_commit | grep "/cards/.*\\.dat$"']
    stdout, stderr, code = run_command(command)
    if code != 0:
        logger.error('No changed files or error: %s', stderr)
        return []

    files = [line.strip() for line in stdout.split('\n') if line.strip()]
    files = [f'{CARDS_REPO_DIR}/{f}' for f in files]
    logger.debug('Changed files:\n  %s', '\n  '.join(files))
    return files


def get_existing_files(filelist):
    """
    For a list of paths, return only those paths that point to an existing file
    (filter-out no longer existing files)
    """
    return [f for f in filelist if os.path.isfile(f)]


def make_script(dataset_name):
    """
    Make a script that should be run in batch system
    """
    cmd = ['#!/usr/bin/env bash',
           'export HOME=$(pwd)',
           'wget https://cms-project-generators.web.cern.ch/cms-project-generators/pdmvtest.tar.gz',
           'tar -xvf pdmvtest.tar.gz',
           'rm pdmvtest.tar.gz',
           'cd genproductions/bin/MadGraph5_aMCatNLO/',
           f'cp -r ../../../{dataset_name}/ .',
           f'./gridpack_generation.sh {dataset_name} {dataset_name}',
           f'mv {dataset_name}*xz ../../../']
    return '\n'.join(cmd)


def make_job_description(dataset_name):
    """
    Make an HTCondor job description file
    """
    dsc = ['universe              = vanilla',
           f'executable           = {dataset_name}.sh',
           f'output               = {dataset_name}.out',
           f'error                = {dataset_name}.err',
           f'log                  = {dataset_name}.log',
           f'transfer_input_files = {dataset_name}',
           'should_transfer_files = YES',
           'when_to_transfer_output = ON_EXIT',
           '+MaxWallTimeMins      = 120',
           'RequestCpus           = 8',
           'RequestMemory         = 16000',
           '+accounting_group     = highprio',
           '+AccountingGroup      = "highprio.pdmvserv"',
           '+AcctGroup            = "highprio"',
           '+AcctGroupUser        = "pdmvserv"',
           '+DESIRED_Sites        = "T2_CH_CERN"',
           '+REQUIRED_OS          = "rhel7"',
           'leave_in_queue        = JobStatus == 4 && (CompletionDate =?= UNDEFINED ||' # same line
           '                        ((CurrentTime - CompletionDate) < 7200))',
           # Lines below are used in monitoring
           '+CMS_Type             = "test"',
           '+CMS_JobType          = "PdmVGridpack"',
           '+CMS_TaskType         = "PdmVGridpack"',
           '+CMS_SubmissionTool   = "Condor_SI"',
           '+CMS_WMTool           = "Condor_SI"',
           'queue']
    return '\n'.join(dsc)


def get_files(directory):
    """
    Return all files in given directory
    """
    return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]


def get_card_name(directory):
    """
    Get card name of directory from _proc_card.dat or _run_card.dat file
    """
    files = get_files(directory)
    card_files = [f for f in files if f.endswith(('_proc_card.dat', '_run_card.dat'))]
    if not card_files:
        return None

    return card_files[0].replace('_proc_card.dat', '').replace('_run_card.dat', '')


def create_jobs_for_cards(filelist):
    """
    Create job files and submit them for given list of changed files
    """
    # Get list of directories
    directories = sorted(list(set(os.path.dirname(f) for f in filelist)))

    logger.info('Directories: %s', directories)
    for directory in directories:
        logger.info('Working on %s', directory)
        card_name = get_card_name(directory)
        logger.info('Got card name: %s', card_name)
        if not card_name:
            continue

        timestamp = int(time.time())
        local_dir = f'/data/srv/multi-validation/tests_vocms0500/jobs/{timestamp}_{card_name}'
        os.makedirs(local_dir, exist_ok=True)
        with open(f'{local_dir}/{card_name}.sh', 'w') as script_file:
            script_file.write(make_script(card_name))

        with open(f'{local_dir}/{card_name}.sub', 'w') as condor_file:
            condor_file.write(make_job_description(card_name))

        os.makedirs(f'{local_dir}/{card_name}/', exist_ok=True)
        run_command([f'cp -R {directory}/* {local_dir}/{card_name}/',
                     f'chmod +x {local_dir}/{card_name}.sh'])
        logger.info('Submitting %s...', card_name)
        out, err, code = run_command([f'cd {local_dir}',
                                      f'condor_submit {card_name}.sub'])
        logger.info('Submission result:\n%s\n%s\n%s', out, err, code)


def run():
    """
    Main function
    """
    jobs = get_jobs_in_condor()
    card_names = get_new_cards()
    logger.info('Changed cards: %s', card_names)
    card_names = get_existing_files(card_names)
    logger.warning('Hardcoded card name!')
    card_names = ['GridpackExtravaganza/Run3_2022/DY_Beep_BoopLO/cards/pdmvtestLO_proc_card.dat']
    logger.info('Existing cards: %s', card_names)
    create_jobs_for_cards(card_names)


if __name__ == '__main__':
    run()
