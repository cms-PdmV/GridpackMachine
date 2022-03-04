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

cards_path = os.path.join('cards', args.directory, args.process, dataset_name)
tmp_path = os.path.join('/tmp', 'gridpacks', f'{dataset_name}_{int(time.time())}')

logger.debug('Campaign path %s', campaign_path)
logger.debug('Template path %s', template_path)
logger.debug('PDF path %s', pdf_path)
logger.debug('Cards path %s', cards_path)
logger.debug('TMP directory %s', tmp_path)

CORES = 8

with open(os.path.join(cards_path, f'{dataset_name}.json')) as input_file:
    dataset_dict = json.load(input_file)

logger.debug('Dataset "%s" dict:\n%s', dataset_name, json.dumps(dataset_dict, indent=2))

# Make the /tmp workspace
logger.debug('Making %s...', tmp_path)
pathlib.Path(tmp_path).mkdir(parents=True, exist_ok=True)


def prepareDefaultCard():
    os.system(f'cp {cards_path}/*.dat {tmp_path}')


def prepareRunCard():
    run_card_path = os.path.join(template_path, "run_card")
    run_card_file_name = os.path.join(tmp_path, f'{dataset_name}_run_card.dat')
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

def prepareCustomizeCard():

    customizecardPath = os.path.join(datasetName, f"{datasetName}_customizecards.dat")

    schemeCard = datasetObject["scheme"]
    schemecardPath = os.path.join(templatePath, "scheme", schemeCard)

    os.system(f"cat {schemecardPath} > {customizecardPath}")

    os.system(f"echo \"\"  >> {customizecardPath}")
    os.system(f"echo \"# User settings\" >> {customizecardPath}")
    for l in datasetObject["user"]:
        os.system(f"echo {l} >> {customizecardPath}")

def prepareWrapper():

    os.system(f"tar -czvf cardsPdmV_{datasetName}.tar.xz {datasetName}")

    wrapper = open(f"runPdmV_{datasetName}.sh", "w")

    genproductions = campaignObject["genproductions"]
    genproductionsLink = f"https://github.com/sihyunjeon/genproductions/archive/refs/tags/{genproductions}.tar.gz"

    wrapper.write("#!/usr/bin/env bash\n")
    wrapper.write("export HOME=`pwd`\n")
    wrapper.write(f"wget {genproductionsLink} \n")
    wrapper.write(f"tar -xf {genproductions}.tar.gz\n")
    wrapper.write(f"rm {genproductions}.tar.gz\n")
    wrapper.write(f"mv genproductions-{genproductions} genproductions\n")
    wrapper.write(f"mv cardsPdmV_{datasetName}.tar.xz genproductions/bin/MadGraph5_aMCatNLO/\n")
    wrapper.write("cd genproductions\n")
    wrapper.write("git init\n")
    wrapper.write(f"cd bin/MadGraph5_aMCatNLO/\n")
    wrapper.write(f"tar -xf cardsPdmV_{datasetName}.tar.xz\n")
    wrapper.write("mkdir -p cardsPdmV\n")
    wrapper.write(f"mv {datasetName} cardsPdmV/\n")
    wrapper.write(f"./gridpack_generation.sh {datasetName} cardsPdmV/{datasetName}\n")
    wrapper.write(f"mv {datasetName}*.xz ../../../\n")
    wrapper.close()

    os.system(f"chmod a+x runPdmV_{datasetName}.sh")

def prepareJDSFile():

    jdsfile = open(f"runPdmV_{datasetName}.jds", "w")
    jdsfile.write(f"executable = runPdmV_{datasetName}.sh\n")
    jdsfile.write(f"transfer_input_files = cardsPdmV_{datasetName}.tar.xz\n")
    jdsfile.write("when_to_transfer_output = on_exit\n")
    jdsfile.write("+JobFlavour = \"testmatch\"\n")
    jdsfile.write(f"+JobBatchName = \"{datasetName}\"\n")
    jdsfile.write(f"output = {datasetName}.stdout\n")
    jdsfile.write(f"error = {datasetName}.stderr\n")
    jdsfile.write(f"log = {datasetName}.stdlog\n")
    jdsfile.write(f"RequestCpus = {CORES}\n")
    jdsfile.write(f"RequestMemory = {CORES * 2000}\n")
    jdsfile.write("should_transfer_files = yes\n")
    jdsfile.write("queue\n")
    jdsfile.close()

def main():

    prepareDefaultCard()
    prepareRunCard()
    prepareCustomizeCard()
    prepareWrapper()
    prepareJDSFile()
    os.system(f"rm -rf {datasetName}")
#   os.system(f"condor_submit runPdmV_{datasetName}.jds")

if __name__ == "__main__":
    main()
