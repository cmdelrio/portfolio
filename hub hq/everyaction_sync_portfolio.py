# Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/111507437

# For each hub, this script takes all of the contacts added to HQ sheet since the last time this script ran
# successfully for that hub, and subscribes them to their EveryAction committee. The control table that this script
# references is sunrise.hq_ea_sync_control_table. Upsert errors are logged in sunrise.hq_ea_sync_errors and all other
# errors are logged in Sunrise.hub_hq_errors


import json
import time
from parsons import GoogleSheets, Redshift, Table, VAN
import gspread
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
import logging
from datetime import timezone, timedelta
import datetime
import os


# Set up logger
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')

# Set environ using civis credentials from container script
os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'

# Load redshift and VAN credentials
rs = Redshift()
api_keys = json.loads(os.environ['EVERYACTION_KEYS_PASSWORD'])

# Load google credentials for parsons
creds = json.loads(os.environ['GOOGLE_JSON_CRED_PASSWORD'])  # Load JSON credentials
parsons_sheets = GoogleSheets(google_keyfile_dict=creds)  # Initiate parsons GSheets class

# Set up google sheets connection for gspread package
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
gspread_client = gspread.authorize(credentials)




def get_hq(spreadsheet_id: str):
    """
    Get all records from hub's HQ
    :param spreadsheet_id: spreadsheet ID for the hub's HQ
    :return: Parson's table of all of the records
    """
    # Connect to the hq with gspread
    hq = gspread_client.open_by_key(spreadsheet_id)
    # Connect to the set up worksheet via gspread
    hq_worksheet = hq.worksheet('Hidden HQ')
    hq_lists = hq_worksheet.get_all_values()
    hq_table = Table(hq_lists[2:])
    return hq_table

def subscribe_to_ea():
    """
    Upsert (i.e. findOrCreate) new HQ contacts into hub's EveryAction committee
    :return: None
    """
    # Try
    for contact in new_hq_contacts:
        json = {
      'firstName': contact['First Name'],
      "lastName": contact['Last Name'],
      "emails":
            [{"email": contact['Email'],
            "isSubscribed":'true'}]
        }
        #Except (need to figure out what kind of errors I'll get here)
        try:
            van.upsert_person_json(json)
            time.sleep(.5)
        except Exception as e:
            response = str(e)
            exceptiondata = traceback.format_exc().splitlines()
            exception = exceptiondata[len(exceptiondata)-1]
            upsert_errors.append([str(date.today()), hub['hub_name'], contact['First Name'],contact['Last Name'],
                                  contact['Email'], response[:999], exception[:999]])
#  traceback.format_exc(chain = False)]

def last_successful_syncs():
    """
    Get dates of the last time the HQ > EA sync ran successfully for each hub
    :return: Parson's table of dates of the last time the HQ > EA sync ran successfully for each hub
    """
    sql = f'''
SELECT
hub
, MAX(date_of_ea_sync_success::datetime)::text AS date 
FROM sunrise.hq_ea_sync_control_table
GROUP BY hub
'''
    date_tbl = rs.query(sql)
    return date_tbl

def main():
    hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'cron job')
    last_successful_sync_tbl = last_successful_syncs()

    # Open errors tables
    upsert_errors = [['date', 'hub', 'first', 'last', 'email', 'error', 'traceback']]
    hq_errors =[['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]
    control_table_update = [['hub', 'date_of_ea_sync_success']]

    for hub in hubs:
        # connect to hubs EveryAction committee
        van = VAN(api_key = api_keys[hub['hub_name']], db='EveryAction')
        # Get hub's HQ
        hq = get_hq(hub['spreadsheet_id'])
        # Get last time sync succeeded for this hub
        try:
            date_str = last_successful_sync_tbl.select_rows(lambda row: row.hub == hub['hub_name'])
            # Convert string to date time format
            date_last_sync = datetime.datetime.strptime(date_str[0]['date'] + ' +00:00', "%Y-%m-%d %H:%M:%S %z")
            # Subset HQ rows to only include contacts that synced since last successful run
            new_hq_contacts = hq.select_rows(lambda row: datetime.datetime.strptime(row['Date Joined'][:19] + ' +00:00',
                                                "%Y-%m-%d %H:%M:%S %z") > date_last_sync)
        # For hubs who haven't had a sync yet
        except KeyError as e:
            error = str(e)
            exceptiondata = traceback.format_exc().splitlines()
            exception = exceptiondata[len(exceptiondata)-1]
            hq_errors.append([str(date.today()), 'everayction_sync', hub['hub_name'], error[:999], exception[:999],
                              'if first time run for hub, hub_name will not be in control table'])
            logger.info(f'''Upserting ALL hq records for {hub['hub_name']} hub''')
            # Upsert all contacts in sheet
            new_hq_contacts = hq

        # Upsert new contacts to EA
        subscribe_to_ea()
        # get now
        now = datetime.datetime.now(timezone.utc)
        now_str = datetime.datetime.strftime(now,'%m/%d/%Y %H:%M:%S')
        # add sync date to control table
        control_table_update.append([hub['hub_name'],now_str])

    rs.copy(Table(control_table_update), 'sunrise.hq_ea_sync_control_table', if_exists='append', distkey='hub',
            sortkey='date_of_ea_sync_success', alter_table=True)
    try:
        rs.copy(Table(hq_errors), 'sunrise.hub_hq_errors', if_exists='append', distkey='hub',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors)-1} errored hubs''')
    except ValueError:
        logger.info('Script executed without issue for all hubs')
    try:
        rs.copy(Table(upsert_errors), 'sunrise.hq_ea_sync_errors', if_exists='append', distkey='error',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors)-1} errored contacts''')
    except ValueError:
        logger.info(f'''All contacts were subscribed to the correct committee without errors''')

if __name__ == '__main__':
    main()

