#!/usr/bin/env python3

import os
import sys
import json
import argparse

parser = argparse.ArgumentParser()

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
parser.add_argument('--tune', '-t',
                    required=True)
parser.add_argument('--lhe',
                    action='store_true',
                    default=False,
                    help='Needness for the ExternalLHEProducer')
parser.add_argument('--concurrent',
                    action='store_true',
                    default=False,
                    help='Needness for the concurrent settings')

args = parser.parse_args()

dataset_name = f'{args.process}_{args.setting}_{args.generator}'
cards_path = os.path.join('cards', args.directory, args.process, dataset_name)

def addExternalLheProducer(fragmentLines):

    with open(os.path.join("cards", "fragment", "template", "ExternalLHEProducer.dat")) as f:
        ll = f.readlines()
        for l in ll:
            fragmentLines += l

    fragmentLines += "\n"

    return fragmentLines


def addHadronizerLines(fragmentLines):

    if os.path.exists(os.path.join("cards", "fragment", "template", f"{args.generator}.dat")):
        with open(os.path.join("cards", "fragment", "template", f"{args.generator}.dat")) as f:
            ll = f.readlines()
            for l in ll:
                fragmentLines += l
    else:
        sys.exit("error : unknown generator")

    fragmentLines += "\n"

    return fragmentLines


def replaceFragmentLines(fragmentLines):

    if args.concurrent:
        fragmentLines = fragmentLines.replace("__generateConcurrently__", "generateConcurrently = cms.untracked.bool(True),")
        fragmentLines = fragmentLines.replace("__concurrent__", "Concurrent")
    else:
        fragmentLines = fragmentLines.replace("__generateConcurrently__", "")
        fragmentLines = fragmentLines.replace("__concurrent__", "")

    fragmentLines = fragmentLines.replace("__tuneName__", args.tune)
    fragmentLines = fragmentLines.replace("__comEnergy__", str(int(args.beamEnergy) * 2))

    with open(os.path.join("cards", "fragment", "import.json")) as input_file:
        import_dict = json.load(input_file)
    try:
        fragmentLines = fragmentLines.replace("__tuneImport__", import_dict[args.tune])
    except:
        sys.exit("error : unknown tune, unable to find import path")

    with open(os.path.join(cards_path, f'{dataset_name}.json')) as input_file:
        dataset_dict = json.load(input_file)

    process_parameters = ""
    for l in dataset_dict["fragment"]:
        process_parameters += f"            '{l}',\n"
    fragmentLines = fragmentLines.replace("__processParameters__", process_parameters)

    return fragmentLines

def main():

    fragmentLines = ""
    fragmentLines += "import FWCore.ParameterSet.Config as cms"
    fragmentLines += "\n\n"

    if args.lhe:
        fragmentLines = addExternalLheProducer(fragmentLines)
    fragmentLines = addHadronizerLines(fragmentLines)
    fragmentLines = replaceFragmentLines(fragmentLines)

    print (fragmentLines)

if __name__ == "__main__":

    main()
