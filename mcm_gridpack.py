import argparse
import sys
sys.path.append('/afs/cern.ch/cms/PPD/PdmV/tools/McM/')
from rest import McM


# McM instance
mcm = None


def create_request(fragment_file, dataset_name, chain, events):
    campaign = chain.split('_')[1]
    print('Creating request in %s' % (campaign))
    request_json = {'pwg': 'GEN',
                    'member_of_campaign': campaign}
    result = mcm.put('requests', request_json)
    print(result)
    prepid = result.get('prepid')
    if not prepid:
        raise Exception('No prepid in %s' % (result))

    print('REQUEST PREPID: %s' % (prepid))
    request = mcm.get('requests', prepid)
    request['dataset_name'] = dataset_name
    with open(fragment_file) as input_file:
        fragment = input_file.read()

    request['fragment'] = fragment
    request['total_events'] = events
    result = mcm.update('requests', request)
    print(result)


def main():
    parser = argparse.ArgumentParser(description='Gridpack Machine -> McM')
    parser.add_argument('--dev', default=False, action='store_true')
    parser.add_argument('--chain')
    parser.add_argument('--dataset')
    parser.add_argument('--fragment')
    parser.add_argument('--events', type=int)
    args = vars(parser.parse_args())
    global mcm
    mcm = McM(dev=args['dev'])
    create_request(args['fragment'], args['dataset'], args['chain'], args['events'])


if __name__ == '__main__':
    main()
