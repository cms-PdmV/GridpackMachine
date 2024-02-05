"""
This module is used for the integration between
GridpackMachine and McM applications. After a Gridpack
is created, it is used to create a McM request
"""
import argparse
import sys
sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')

#pylint: disable=wrong-import-position
from rest import McM


# McM instance
mcm = None #pylint: disable=invalid-name


def create_request(fragment_file, dataset_name, chain, events, tag, generator):
    """
    Creates a request in the McM application using the produced Gridpack
    and its fragment.

    Args:
        fragment_file (str): Path to Gridpack's fragment definition.
            This is typically a Python module.
        dataset_name (str): Gridpack's dataset name.
        chain (str): McM chained campaign linked to the Gridpack.
        events (str): Gridpack's number of events.
        tag (str): Tag for the new McM request.
        generator (str): Generator used to create the Gridpack.
    """
    campaign = chain.split('_')[1]
    print(f'Creating request in {campaign}')
    request_json = {'pwg': 'GEN',
                    'member_of_campaign': campaign}
    result = mcm.put('requests', request_json)
    print(result)
    prepid = result.get('prepid')
    if not prepid:
        raise Exception(f'No prepid in {result}')

    print(f'REQUEST PREPID: {prepid}')
    request = mcm.get('requests', prepid)
    request['dataset_name'] = dataset_name
    with open(fragment_file, encoding='utf-8') as input_file:
        fragment = input_file.read()

    request['fragment'] = fragment
    request['total_events'] = events
    request['tags'] = [tag]
    request['mcdb_id'] = 0
    request['generators'] = [generator]
    result = mcm.update('requests', request)
    print(result)


def main():
    """
    Starts sub-module execution for creating a McM request.
    """
    parser = argparse.ArgumentParser(description='Gridpack Machine -> McM')
    parser.add_argument('--dev', default=False, action='store_true')
    parser.add_argument('--chain')
    parser.add_argument('--dataset')
    parser.add_argument('--fragment')
    parser.add_argument('--events', type=int)
    parser.add_argument('--tag')
    parser.add_argument('--generator')
    args = vars(parser.parse_args())
    global mcm #pylint: disable=global-statement
    mcm = McM(dev=args['dev'])
    create_request(
        args['fragment'],
        args['dataset'],
        args['chain'],
        args['events'],
        args['tag'],
        args['generator']
    )


if __name__ == '__main__':
    main()
