import os
import pathlib
import sys
import json
import argparse
import logging
import time



parser = argparse.ArgumentParser()
parser.add_argument('--campaign',
                    required=True,
                    help='Campaign directory')
parser.add_argument('--process', '-p',
                    required=True)
parser.add_argument('--setting', '-s',
                    required=True)
parser.add_argument('--generator', '-g',
                    required=True)
parser.add_argument('--directory', '-d',
                    required=True)
parser.add_argument('--beamEnergy', '-e',
                    required=True)
parser.add_argument('--pythiaTune', '-t',
                    required=True)
parser.add_argument('--genproductions', '-i',
                    required=True)
parser.add_argument('--debug',
                    action='store_true',
                    default=False)
args = parser.parse_args()

log_format = '[%(asctime)s][%(levelname)s] %(message)s'
logging.basicConfig(format=log_format, level=logging.DEBUG if args.debug else logging.INFO)
logger = logging.getLogger()

logger.debug('Arguments %s', args)

campaign_path = os.path.join('campaigns', args.campaign)
template_path = os.path.join(campaign_path, 'template', args.directory)
pdf_path = os.path.join(campaign_path, 'PDF')
# Not really a dataset name, but almost
dataset_name = f'{args.process}_{args.setting}_{args.generator}'
# Link to GEN archive
gen_archive_link = f"https://github.com/sihyunjeon/genproductions/archive/refs/tags/{args.genproductions}.tar.gz"

cards_path = os.path.join('cards', args.directory, args.process, dataset_name)
tmp_path = os.path.join('/tmp', 'gridpacks', f'{dataset_name}_{int(time.time())}')
tmp_cards_path = os.path.join(tmp_path, 'cards')

# Files needed for condor
script_name = f'GRIDPACK_{dataset_name}.sh'
jds_name = f'GRIDPACK_{dataset_name}.jds'

logger.debug('Campaign path %s', campaign_path)
logger.debug('Template path %s', template_path)
logger.debug('PDF path %s', pdf_path)
logger.debug('Cards path %s', cards_path)
logger.debug('TMP directory %s', tmp_path)
logger.debug('TMP cards directory %s', tmp_cards_path)

CORES = 8
MEMORY = CORES * 2000

with open(os.path.join(cards_path, f'{dataset_name}.json')) as input_file:
    dataset_dict = json.load(input_file)

logger.debug('Dataset "%s" dict:\n%s', dataset_name, json.dumps(dataset_dict, indent=2))

# Make the /tmp workspace
logger.debug('Making %s...', tmp_path)
pathlib.Path(tmp_cards_path).mkdir(parents=True, exist_ok=True)


def prepare_default_card():
    logger.debug('Copying %s/*.dat to %s', cards_path, tmp_cards_path)
    os.system(f'cp {cards_path}/*.dat {tmp_cards_path}')


def prepare_run_card():
    run_card_path = os.path.join(template_path, "run_card")
    run_card_file_name = os.path.join(tmp_cards_path, f'{dataset_name}_run_card.dat')
    if "amcatnlo" in template_path.lower():
        os.system(f"cp {run_card_path}/NLO_run_card.dat {run_card_file_name}")
    elif "madgraph" in template_path.lower():
        os.system(f"cp {run_card_path}/LO_run_card.dat {run_card_file_name}")
    else:
        logger.error('Could not find "amcatnlo" or "madgraph" in "%s"', dataset_name)
        sys.exit(1)

    with open(run_card_file_name) as input_file:
        logger.debug('Reading %s...', run_card_file_name)
        run_card_file = input_file.read()

    run_card_file = run_card_file.replace('$ebeam1', args.beamEnergy)
    run_card_file = run_card_file.replace('$ebeam2', args.beamEnergy)
    for key, value in dataset_dict.get('run_card', {}).items():
        key = f'${key}'
        logger.debug('Replacing "%s" with "%s" in %s', key, value, run_card_file_name)
        run_card_file = run_card_file.replace(key, value)

    with open(run_card_file_name, 'w') as output_file:
        logger.debug('Writing %s...', run_card_file_name)
        output_file.write(run_card_file)


def prepare_customize_card():
    scheme_name = dataset_dict.get('scheme')
    if not scheme_name:
        return

    customize_card_path = os.path.join(tmp_cards_path, f'{dataset_name}_customizecards.dat')
    scheme_path = os.path.join(template_path, 'scheme', scheme_name)
    logger.debug('Reading scheme file %s', scheme_path)
    with open(scheme_path) as scheme_file:
        scheme = scheme_file.read()

    scheme = scheme.split('\n')
    scheme += ['', '# User settings']
    for user_line in dataset_dict.get('user', []):
        logger.debug('Appeding %s', user_line)
        scheme += [user_line]

    scheme = '\n'.join(scheme)
    logger.debug('Writing customized scheme file %s', customize_card_path)
    with open(customize_card_path, 'w') as scheme_file:
        scheme_file.write(scheme)


def prepare_card_archive():
    os.system(f"tar -czvf {tmp_path}/cards.tar.gz -C {tmp_path} cards")


def prepare_script():
    command = ['#!/bin/sh',
               'export HOME=$(pwd)',
               'export ORG_PWD=$(pwd)',
               f'wget {gen_archive_link} -O gen_productions.tar.gz',
               f'tar -xzf gen_productions.tar.gz',
               f'mv genproductions-{args.genproductions} genproductions',
               f'mv cards.tar.gz genproductions/bin/{args.directory}',
               f'cd genproductions/bin/{args.directory}',
               f'tar -xzf cards.tar.gz',
               f'./gridpack_generation.sh {dataset_name} cards',
               f'mv {dataset_name}*.xz $ORG_PWD']

    script_path = os.path.join(tmp_path, script_name)
    logger.debug('Writing sh script to %s', script_path)
    with open(script_path, 'w') as script_file:
        script_file.write('\n'.join(command))

    os.system(f"chmod a+x {script_path}")


def prepare_jds_file():
    jds = [f'executable             = {script_name}',
           'transfer_input_files    = cards.tar.gz',
           'when_to_transfer_output = on_exit',
           'should_transfer_files   = yes',
           '+JobFlavour             = "testmatch"',
           f'+JobBatchName          = "{dataset_name}"',
           'output                  = log.out',
           'error                   = log.err',
           'log                     = job_log.log',
           f'RequestCpus            = {CORES}',
           f'RequestMemory          = {MEMORY}',
           '+accounting_group       = highprio',
           '+AccountingGroup        = "highprio.pdmvserv"',
           '+AcctGroup              = "highprio"',
           '+AcctGroupUser          = "pdmvserv"',
           '+DESIRED_Sites          = "T2_CH_CERN"',
           '+REQUIRED_OS            = "rhel7"',
           'leave_in_queue          = JobStatus == 4 && (CompletionDate =?= UNDEFINED || ((CurrentTime - CompletionDate) < 7200))',
           '+CMS_Type               = "test"',
           '+CMS_JobType            = "PdmVGridpack"',
           '+CMS_TaskType           = "PdmVGridpack"',
           '+CMS_SubmissionTool     = "Condor_SI"',
           '+CMS_WMTool             = "Condor_SI"',
           'queue']

    jds_path = os.path.join(tmp_path, jds_name)
    logger.debug('Writing JDS to %s', jds_path)
    with open(jds_path, 'w') as jds_file:
        jds_file.write('\n'.join(jds))


def main():

    prepare_default_card()
    prepare_run_card()
    prepare_customize_card()
    prepare_card_archive()
    prepare_script()
    prepare_jds_file()
    # os.system(f"rm -rf {datasetName}")
    # os.system(f"condor_submit runPdmV_{datasetName}.jds")

if __name__ == "__main__":
    main()
