#!/usr/bin/env python3

import os
import sys
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

    #FIXME "__tuneImport__"

    fragmentLines = fragmentLines.replace("__tuneName__", args.tune)
    fragmentLines = fragmentLines.replace("__beamEnergy__", args.beamEnergy)

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
