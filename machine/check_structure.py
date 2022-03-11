import sys
import csv
import os
import json
import logging


logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
logger = logging.getLogger()
errors = 0


campaigns = [c for c in os.listdir('campaigns') if os.path.isdir(os.path.join('campaigns', c))]
logger.info('Found %s campaigns', len(campaigns))
for campaign_name in campaigns:
    logger.info('Checking %s', campaign_name)
    # Get campaign CSV
    campaign_path = os.path.join('campaigns', campaign_name)
    campaign_csv_path = os.path.join(campaign_path, f'{campaign_name}.csv')
    if not os.path.exists(campaign_csv_path):
        logger.error('Missing %s csv at %s', campaign_name, campaign_csv_path)
        errors += 1

    with open(campaign_csv_path, newline='') as csv_file:
        csv_rows = list(csv.DictReader(csv_file, delimiter=','))

    logger.info('Found %s entries in %s', len(csv_rows), campaign_csv_path)
    for i, csv_row in enumerate(csv_rows):
        process = csv_row['process']
        setting = csv_row['setting']
        generator = csv_row['generator']
        directory = csv_row['directory']
        dataset_name = f'{process}_{setting}_{generator}'
        logger.info('Checking entry %s - %s/%s', i + 1, directory, dataset_name)
        # Check if cards directory exists
        cards_path = os.path.join('cards', directory, process, dataset_name)
        if not os.path.exists(cards_path):
            logger.error('Missing directory %s', cards_path)
            errors += 1

        # Check if cards _proc_card.dat exists
        proc_card_path = os.path.join(cards_path, f'{dataset_name}_proc_card.dat')
        if not os.path.exists(proc_card_path):
            logger.error('Missing card file %s', proc_card_path)
            errors += 1

        # Check if cards JSON exists
        card_json_path = os.path.join(cards_path, f'{dataset_name}.json')
        if not os.path.exists(card_json_path):
            logger.error('Missing card JSON %s', card_json_path)
            errors += 1

        # Check if cards JSON is a valid file
        card_json = {}
        try:
            with open(card_json_path) as json_input:
                card_json = json.load(json_input)
        except Exception as ex:
            logger.error('Error parsing %s: %s', card_json_path, str(ex))
            errors += 1

        # Check if run card templates exist for campaign
        campaign_run_card_path = os.path.join(campaign_path, 'template', directory, 'run_card')
        if not os.path.exists(campaign_run_card_path):
            logger.error('Missing campaign run card templates directory %s', campaign_run_card_path)
            errors += 1

        # check if schemes exist for campaign
        campaign_scheme_path = os.path.join(campaign_path, 'template', directory, 'scheme')
        if not os.path.exists(campaign_scheme_path):
            logger.error('Missing campaign scheme directory %s', campaign_scheme_path)
            errors += 1

        # Check if scheme file, specified in card JSON exists
        scheme = card_json.get('scheme')
        if scheme:
            scheme_file_path = os.path.join(campaign_scheme_path, scheme)
            if not os.path.exists(scheme_file_path):
                logger.error('Missing scheme file %s', scheme_file_path)
                errors += 1

if errors:
    logger.error('Found %s errors', errors)
    sys.exit(1)

logger.info('OK')
