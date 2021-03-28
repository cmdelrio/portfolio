# Civis container script: https://platform.civisanalytics.com/spa/#/scripts/containers/111401507

# This script gets event attendance data from mobilize for each hub in the cron job sheet as a table of unique contacts
# with their contact info and event attendance history. It compares those contacts to the contacts in HQ Spreadsheet
# for that hub and updates event attendance history for any contacts that already exist in HQ sheet, and appends any
# new Mobilize contacts that don't have a match in HQ sheet. Match is based on email.
# Errors are logged in sunrise.hub_hq_errors

# Import necessary packages
import json
from parsons import GoogleSheets, Redshift, Table
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from datetime import timezone, timedelta
import datetime
import os

##### Set up logger #####
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


# Set up environment and load credentials
rs = Redshift()  # Redshift

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

# Put HQ columns into a dictionary to make it easy to reference
hidden_hq_columns = {
    'date_joined': 4, 'first_name': 0, 'last_name': 1, 'email': 2, 'phone': 3, 'total_signups': 5,
    'total_attendances': 6, 'first_signup': 7, 'first_attendance': 8, 'days_since_last_signup': 9,
    'days_since_last_attendance': 10, 'status':11
}

# Get cron job spreadsheet
hubs = parsons_sheets.get_worksheet('1ESXwSfjkDrgCRYrAag_SHiKCMIgcd1U3kz47KLNpGeA', 'cron job')

# Create errors list of lists to populate and push to redshift at the end
hq_errors =[['date', 'script', 'hub', 'error', 'traceback', 'other_messages']]

def connect_to_hq(hub: dict):
    """
    Connect to HQ worksheet for hub
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: A worksheet object of gspread class worksheet
    """
    # connect to spreadsheet with spread
    spreadsheet = gspread_client.open_by_key(hub['spreadsheet_id'])
    # Get Hidden HQ as list of lists
    hidden_hq_worksheet = spreadsheet.worksheet('Hidden HQ')
    return hidden_hq_worksheet


def get_mobilize_data(hub: dict):
    """
    Get Mobilize event attendance data for hub
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :return: A dictionary of dictionaries where each key is a unique email and each item is a row of info from Mobilize
    """
    # Get Mobilize data -- query returns a table of deduped contacts and their event attendance history
    event_attendance_sql = f'''
with 
-- deals with duplicate
most_recent as 
(
    select
        ppl.created_date,
  		ppl.user_id as person_id, 
        ppl.user__given_name as first_name,
        ppl.user__family_name as last_name,
        ppl.user__email_address as email,
        ppl.user__phone_number as phone_number,
        ppl.status,
        ppl.timeslot_id,
        ppl.event_id,
        ppl.attended,
        ppl.start_date,
  		events.id,
  		events.title,
        row_number() over (partition by ppl.id order by ppl.created_date::date desc) = 1 as is_most_recent
  	from sunrise_mobilize.participations ppl
    left join sunrise_mobilize.events events on ppl.event_id = events.id
    where events.creator__email_address ilike '{hub['hub_email']}'
),


--Get unique signups
signups as
(
    select * from most_recent where is_most_recent = true
)

-- get unique people rows from signups
select 
    max(first_name) as first_name,
    max(last_name) as last_name,
    email,
    max(phone_number) as phone,
    min(created_date)::text as date_joined,
    count(*) as total_signups,
    sum
    (
    case 
        when attended = true then 1
        else 0
    end
    ) as total_attendances,
    min(start_date::date)::text as first_signup,
    min
        (
        case 
            when attended = true then start_date::date
            else null
        end
        )::text as first_attendance,
    datediff(day,max(start_date)::date,getdate()) as days_since_last_signup,
    datediff
        (
        day
        ,max
            (
            case 
                when attended = true then start_date
                else null
            end
            )::date
        ,getdate()) as days_since_last_attendance
from signups
group by email
order by date_joined
'''
    # Send query to mobilize
    mobilize_data = rs.query(sql=event_attendance_sql)
    if mobilize_data.num_rows == 0:
        return
    # Store mobilize rows in a dictionary where each row's key is an email (used for matching)
    else:
        mobilize_dict = {i['email']: i for i in mobilize_data}
        return mobilize_dict


def mobilize_updates(hub: dict, mobilize_dict: dict, hidden_hq: list, hidden_hq_worksheet, hidden_hq_columns):
    """
    Each row/list from the HQ is checked for a match in the mobilize data using email. A new list of lists is created
    where each list is a person's event attendance record from mobilize. If there is an email match then the resulting
    list/row for that contact contains event attendance data and the row for that contact is removed  from the mobilize
    data dictionary (which we append to Hidden HQ later). If there is no match, then the resulting row/list will have
    four empty values. The rows/lists are ordered within the outer list exactly how the Hidden HQ is ordered so that we
    can push the list back to the HQ and have each event attendance record line up with the correct contact in the
    hidden HQ. Finally the updates are pushed to the Hidden HQ and the mobilize rows for which there were no matches in
    the HQ are returned as a parson's table
    :param hub: dictionary for that hub from set up sheet, retrieved by parsons
    :param mobilize_dict: dictionary of mobilize data where each key is a unique email
    :param hidden_hq: a list of lists, where each innter list is a row from the hub's HQ
    :param hidden_hq_columns: dictionary indicating the index of each HQ column in the actual spreadsheet
    :param hidden_hq_worksheet: the hq worksheet, which is a gspread class of object
    :return: A parson's table of mobilize records without matches in the HQ
    """

    # Create a list of the event sign up/attencance summary fields we're going to attenpt to update in the HQ
    update_items = list(hidden_hq_columns.keys())
    update_items = update_items[:hidden_hq_columns['status']]
    # Open a list for the updates, which we will fill with lists, one for each contact.
    event_attendance_updates = []
    now = datetime.datetime.now(timezone.utc)
    sevendays = datetime.timedelta(days=7)
    sixtydays = datetime.timedelta(days=60)
    # For each row in the hub_hq, if the email is in the mobilize data, then update the appropriate fields/items,
    # otherwise,append a list of blank values
    for hq_row in hidden_hq:
        # Update Hidden HQ records that have a match in the retrieved mobilize data and remove from the mobilize data
        try:
            # Update each field/list item from the update_items for the match. This will create a whole update list/row
            # substitute mobilize values for hq values
            for i in update_items:
                # If the email address of the hq row exists in the mobilize data, append the correct event attendance
                # value to the list
                hq_row[hidden_hq_columns[i]] = mobilize_dict[hq_row[hidden_hq_columns['email']]][i]
            # Assign status based on event sign up metrics
            # Start by getting date joined from HQ
            date_joined = datetime.datetime.strptime(hq_row[hidden_hq_columns['date_joined']][:19] + ' +00:00',
                                                     "%Y-%m-%d %H:%M:%S %z")
            if now - date_joined <= sevendays:
                status = 'HOT LEAD'
            elif sevendays < now - date_joined <= sixtydays:
                status = "Prospective/New Member"
            elif mobilize_dict[hq_row[hidden_hq_columns['email']]]['total_signups'] > 2 and \
                     mobilize_dict[hq_row[hidden_hq_columns['email']]]['days_since_last_signup'] < 60:
                status = 'Active Member'
            elif mobilize_dict[hq_row[hidden_hq_columns['email']]]['total_signups'] > 2 and \
                     mobilize_dict[hq_row[hidden_hq_columns['email']]]['days_since_last_signup'] >= 60:
                status = 'Inactive Member'
            elif mobilize_dict[hq_row[hidden_hq_columns['email']]]['total_signups'] <= 2 and now - date_joined > sixtydays:
                status = 'Never got involved'
            else:
                status = 'error'
            hq_row[hidden_hq_columns['status']] = status
            # Reduce to fields that need to be updated
            update_row = hq_row[hidden_hq_columns['total_signups']:hidden_hq_columns['status']+1]
            # Add to the update list of lists
            event_attendance_updates.append(update_row)
            # Remove contact from mobilize parson's table dictionary, which will be appended to hidden hq sheet
            del mobilize_dict[hq_row[hidden_hq_columns['email']]]
        # When no match is found, create a list/row with empty values/just retain the value on record (which are empty)

        except KeyError:
            event_attendance_updates.append(hq_row[hidden_hq_columns['total_signups']:
                                                   hidden_hq_columns['days_since_last_attendance'] + 1])
    # Send the updates to Hidden HQ
    hidden_hq_worksheet.update('F4:L', event_attendance_updates)

    # Now we convert the remaining Mobilize records, for which no matches were found, and reformat them to a parson's
    # table so that we can append them to the google sheet using the parson's google sheet append method. We also add a
    # value of 'Mobilize' for the source column

    # Convert remainder of mobilize dictionary rows to lists, which will be converted to a parsons table
    columns_to_append = ['first_name', 'last_name', 'email', 'phone', 'date_joined', 'total_signups',
                         'total_attendances', 'first_signup', 'first_attendance', 'days_since_last_signup', 'days_since_last_attendance']
    # create list of lists
    mobilize_data_append = [[mobilize_dict[i][value] for value in columns_to_append] for i in mobilize_dict]
    # insert column headers
    mobilize_data_append.insert(0,['date_joined', 'first_name', 'last_name', 'email', 'phone', 'total_signups',
                         'total_attendances', 'first_signup', 'first_attendance', 'days_since_last_signup', 'days_since_last_attendance'])
    # convert to parsons table
    mobilize_parsons_append = Table(mobilize_data_append)
    # Add column for status and assign value HOT LEAD since this script is running everyday and only people who just
    # signed up for their first event will be in this append table. The updates section of the script will update their
    # status in the future
    mobilize_parsons_append.add_column('status','HOT LEAD')
    return mobilize_parsons_append

def main():
    for hub in hubs:
        # Connect to the hub's spreadsheet
        hidden_hq_worksheet = connect_to_hq(hub)
        # Get hidden hq table
        hidden_hq = hidden_hq_worksheet.get_all_values()
        # Remove first 3 rows (column headers and instuctions/tips)
        hidden_hq = hidden_hq[3:]
        # Send for Mobilize Data
        mobilize_dict = get_mobilize_data(hub)
        # if not mobilize data
        if mobilize_dict is None:
            hq_errors.append([str(date.today()), 'mobilize_script', hub['hub_name'],
                              f'''No mobilize events associated with hub email {hub['hub_email']}''', 'NA', 'NA'])
            logger.info(f'''No mobilize events associated with hub {hub['hub_name']} email {hub['hub_email']}''')
            continue
        else:
            # Try to send mobilize event attendance updates to HQ and get the left over mobilize rows for which no
            # matches were found in HQ
            try:
                mobilize_parsons_append = mobilize_updates(hub, mobilize_dict, hidden_hq, hidden_hq_worksheet,
                                                           hidden_hq_columns)
            # Append left over mobilize rows to HQ
                try:
                    parsons_sheets.append_to_sheet(hub['spreadsheet_id'], mobilize_parsons_append, 'Hidden HQ')
                except ValueError as e:
                    logger.info(f'''No new mobilize contacts for {hub['hub_name']}''')
                except Exception as e:
                    response = str(e)
                    exceptiondata = traceback.format_exc().splitlines()
                    exception = exceptiondata[len(exceptiondata) - 1]
                    hq_errors.append([str(date.today()), 'mobilize_script', hub['hub_name'],
                                      'Error applying event sign up updates', response[:999], exception[:999]])
                    logger.info(f'''Error appending new mobilize contacts for {hub['hub_name']}''')
            except Exception as e:
                response = str(e)
                exceptiondata = traceback.format_exc().splitlines()
                exception = exceptiondata[len(exceptiondata) - 1]
                hq_errors.append([str(date.today()), 'mobilize_script', hub['hub_name'],
                                  'Error applying event sign up updates', response[:999], exception[:999]])
    try:
        rs.copy(Table(hq_errors), 'sunrise.hub_hq_errors', if_exists='append', distkey='hub',
            sortkey='date', alter_table=True)
        logger.info(f'''{len(hq_errors)-1} errored hubs''')
    except ValueError:
        logger.info('Script executed without issue for all hubs')

if __name__ == '__main__':
    main()
