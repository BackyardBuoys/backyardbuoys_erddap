#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import shutil

import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd
import numpy as np
import json

import datetime

import backyardbuoys_dataaccess as bb_da
import backyardbuoys_general_functions as bb


# In[ ]:


"""
This collection of functions are general
functions that would be used to build the json
files which contain the metadata for each location.

The metadata files include that which is used
to build the netCDF files, information on the 
location ID (including its history, and the
history of the Spotter buoys that have been 
present), and the QARTOD limits for each location.
"""


# # Google API Functions

# In[ ]:


"""
This subset of functions is used to pull
data from the Google Drive API, where
Google sheets contain location metadata
and QARTOD limits.

Note that these are intended to be legacy
processes, as all data pulls should be
migrated to coming from the Backyard Buoys API.
"""


# In[ ]:


def get_auth_dir():
    
    """
    This function identifies the base directory
    that contains the authorization tokens
    
    Function inputs:
    None
    
    Function outputs:
    auth_dir - the directory path to the folder that
               contains the authorization tokens
    """
    
    # Get the current working directory
    basedir = os.path.dirname(__file__)

    # Load in the directory info json
    with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the authorization tokens
    auth_dir = dir_info['auth_token']
        
    return auth_dir


# In[ ]:


def get_user_token():
    
    """
    This function was taken from Google Sheet API tutorials.
    
    It loads in the directory for the authoization tokens,
    and then creates a new authoization taken.
    """

    creds = None
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    
    auth_dir = get_auth_dir()
    
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(os.path.join(auth_dir, 'token.json')):
        creds = Credentials.from_authorized_user_file(os.path.join(auth_dir, 'token.json'), SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
              os.path.join(auth_dir, 'backyardbuoys_google_oauth.json'), SCOPES
          )

        creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(os.path.join(auth_dir, 'token.json'), "w") as token:
            token.write(creds.to_json())

    return


# In[ ]:


def create(title):
    """
    This function was taken from Google Sheet API tutorials.
    
    Creates the Sheet the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    
    auth_dir = get_auth_dir()
    token_path = os.path.join(auth_dir, 'token.json')
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    
    # pylint: disable=maybe-no-member
    try:
        service = build("sheets", "v4", credentials=creds)
        spreadsheet = {"properties": {"title": title}}
        spreadsheet = (
            service.spreadsheets()
            .create(body=spreadsheet, fields="spreadsheetId")
            .execute()
        )
        print(f"Spreadsheet ID: {(spreadsheet.get('spreadsheetId'))}")
        return spreadsheet.get("spreadsheetId")
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


# In[ ]:


def batch_get_values(spreadsheet_id, _range_names):
    """
    This function was taken from Google Sheet API tutorials.
    
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    
    auth_dir = get_auth_dir()
    token_path = os.path.join(auth_dir, 'token.json')
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    
    # pylint: disable=maybe-no-member
    try:
        service = build("sheets", "v4", credentials=creds)
        range_names = [
            # Range names ...
        ]
        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=spreadsheet_id, ranges=_range_names)
            .execute()
        )
        ranges = result.get("valueRanges", [])
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


# In[ ]:


def make_project_metadata_googleapi(loc_id):
    
    """
    This function creates the dataset info json files
    for a given location ID, using the Google API.
    
    The json files include a metadata json and
    a QARTOD limits json
    
    Function inputs:
    loc_id - the location ID for a given dataset
    
    Function outputs:
    successFlag - a flag which indicates whether
                  info jsons were made for this
                  location ID    
    """
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations() 
    meta_df = get_all_google_metadata()
    qc_df = get_all_google_qcdata()
    
    # Find the indice of the matching location ID
    bb_loc_ids= [bb_locs[ii]['loc_id'] for ii in bb_locs.keys()]
    bb_loc_labels = [bb_locs[ii]['label'] for ii in bb_locs.keys()]
    if any([ii == loc_id for ii in bb_loc_ids]):
        bb_ind = np.where([ii == loc_id for ii in bb_loc_ids])[0][0]
    else:
        print('No data found which matches the location ID: ' + loc_id)
        print('Unable to proceed.')
        return False
    
    # Get the metadata for the specific project
    bb_label = bb_loc_labels[bb_ind]
    if any(meta_df['Location Name'] == bb_label):
        meta_ind = np.where(meta_df['Location Name'] == bb_label)[0][0]
    else:
        print('No metadata was found which matched the location ID: ' + loc_id)
        print('Unable to proceed.')
        return False
        
    
    # Make the metadata and QC jsons for the specified project
    make_metadata_json_old(basedir, meta_df.iloc[meta_ind], 
                       bb_loc_labels[bb_ind], loc_id)
    make_qcdata_json(basedir, loc_id, qc_df)
    
    return True


# # Metadata compilation functions

# In[ ]:


def get_all_google_metadata():
    
    """
    This function pulls all the location metadata
    from the Google Sheets API, and returns it
    as a pandas dataframe
    
    Function inputs:
    None
    
    Function outputs:
    meta_df - a pandas dataframe containing the
              location info metadata for all sites
    """
    
    # Load in the info needed to connect to the Google
    # Sheets API
    google_info = bb.load_googleinfo_json()
    
    # Extract out the location of the metadata sheet
    metadata_sheetid = google_info['metadata']
    
    # Pull the metadata from the Google Sheet API
    all_meta = batch_get_values(metadata_sheetid,'A1:N')

    # Restructure the data into a pandas dataframe
    meta_df = pd.DataFrame(columns = all_meta['valueRanges'][0]['values'][0],
                           data=all_meta['valueRanges'][0]['values'][1:])
    
    return meta_df


# In[ ]:


def bb_api_build_contributors(contributors, unique_contributor_list):

    """
    The function takes a string, with a semicolor-separated
    list of contributor information, with each entry containing
    a comma-separated list of the name of the contributor, the
    contributor role identifier, and the URL for the contributor
    and splits it out into a list of each of those fields
    
    Function inputs:
    contributors - a string containing a list of all contributors
                   for a given spotter buoy platform
                   
    Function outputs:
    contributor_names - a list of the names of all contributors
    contributor_roles - a list of the roles of all contributors
    contributor_urls  - a list of the url associated with each
                        contributor
    """
    
    # Build a string of contributor IDs, based
    # on the data taken from the Backyard Buoys API
    
    # Initialize the strings for the contributor fields
    contributor_names = ''
    contributor_roles = ''
    contributor_urls = ''
    
    # Ensure that the contributors fields is a list of contributors
    if (contributors[0] == '[') and (contributors[-1] == ']'):
        
        # Split the list into fields for individual contributors
        contributor_fields = contributors[2:-2].split('},{')

        # Step through each contributor in the list
        for contributor in contributor_fields:
            # Split the fields into the three fields, and extract out the values
            split_fields = contributor.split(',')
            cont_name = split_fields[0][split_fields[0].find('"name":')+7:][1:-1].strip(' ')
            role_name = split_fields[1][split_fields[1].find('"role":')+7:][1:-1].strip(' ')
            url_name = split_fields[2][split_fields[2].find('"url":')+6:][1:-1].strip(' ')
            # If the role field includes "IOS", strip that out
            if 'IOS-' in role_name:
                role_name =role_name[role_name.find('IOS-')+4:]
                
            # Skip this entry if it already exists in the unique contributor list
            if any([name == cont_name for name in unique_contributor_list]):
                continue
            else:                
                unique_contributor_list.append(cont_name)
                
                # Add the contributor name
                contributor_names += cont_name + ', '

                # If the role name has an entry, 
                # add the contributor role name from the field ID;
                # Otherwise, add "None"
                if len(role_name) > 0:
                    contributor_roles += find_contributor_role(role_name) + ', '
                else:
                    contributor_roles += "None, "

                # If there is a url name has an 
                # entry, add this
                # Otherwise, add "None
                if len(url_name) > 0:
                    contributor_urls += url_name + ', '
                else:
                    contributor_urls += "None, "

        # If any entries have been added, 
        # strip off the final ", " from each string
        if len(contributor_names) > 0:
            contributor_names = contributor_names[:-2]
            contributor_roles = contributor_roles[:-2]
            contributor_urls = contributor_urls[:-2]
        
    return contributor_names, contributor_roles, contributor_urls, unique_contributor_list


# In[ ]:


def get_googleapi_contributors(contributors):
    
    """
    The function takes a string, with a semicolor-separated
    list of contributor information, with each entry containing
    a comma-separated list of the name of the contributor, the
    contributor role identifier, and the URL for the contributor
    and splits it out into a list of each of those fields
    
    Function inputs:
    contributors - a string containing a list of all contributors
                   for a given spotter buoy platform
                   
    Function outputs:
    contributor_names - a list of the names of all contributors
    contributor_roles - a list of the roles of all contributors
    contributor_urls  - a list of the url associated with each
                        contributor
    """
    
    # Build a string of contributor IDs, based
    # on the data taken from the Google Sheet API
    
    if ';' in contributors:
        contributors = [ii.strip('\n').strip(' ') for ii in contributors.split(';')]
    else:
        contributors = [ii.strip(' ') for ii in contributors.split('\n')]

    contributor_names = ''
    contributor_roles = ''
    contributor_urls = ''
    for contributor in contributors:
        contributor_str = contributor.split(',')
        if len(contributor_str) > 0:
            contributor_names += contributor_str[0].strip(' ') + ', '
        else:
            continue

        if len(contributor_str) > 1:
            contributor_roles += find_contributor_role(contributor_str[1].strip(' ')) + ', '
        else:
            contributor_roles += "None, "
            contributor_urls += "None, "

        if len(contributor_str) > 2:        
            contributor_urls += contributor_str[2].strip(' ') + ', '
        else:
            contributor_urls += "None, "

    if len(contributors) > 0:
        contributor_names = contributor_names[:-2]
        contributor_roles = contributor_roles[:-2]
        contributor_urls = contributor_urls[:-2]
        
    return contributor_names, contributor_roles, contributor_urls


# In[ ]:


def find_contributor_role(contributor_role):
    """
    Find the matching contributor role name from the role ID
    Source: https://vocab.nerc.ac.uk/collection/G04/current/
    
    Function inputs:
    contributor_role     - the numeric identifier for a given role
    
    Function outputs:
    contrubutor_role_txt - the text label corresponding to
                           the numerical contributor role identifier
    """
    
    contributor_dict = {
        '011': 'author',
        '013': 'coAuthor',
        '014': 'collaborator',
        '018': 'contributor',
        '002': 'custodian',
        '005': 'distributor',
        '015': 'editor',
        '019': 'distributor',
        '016': 'mediator',
        '006': 'originator',
        '003': 'owner',
        '007': 'pointOfContact',
        '008': 'principalInvestigator',
        '009': 'processor',
        '010': 'publisher',
        '001': 'resourceProvider',
        '017': 'rightsHolder',
        '012': 'sponsor',
        '020': 'stakeholder',
        '004': 'user'
    }
    
    contributor_keys = contributor_dict.keys()
    if any([ii == contributor_role for ii in contributor_keys]):
        return contributor_dict[contributor_role]
    else:
        return contributor_role


# In[ ]:


def get_lat_lon_bounds(meta_series):
    
    northbnd = meta_series['Northern_bound']
    if ',' in northbnd:
        northbnd = northbnd.split(',')[0].strip(' ')
    southbnd = meta_series['Southern_bound']
    if ',' in southbnd:
        southbnd = southbnd.split(',')[0].strip(' ')
    westbnd = meta_series['Western_bound']
    if ',' in westbnd:
        westbnd = westbnd.split(',')[1].strip(' ')
    eastbnd = meta_series['Eastern_bound']
    if ',' in eastbnd:
        eastbnd = eastbnd.split(',')[1].strip(' ')
        
    return northbnd, southbnd, westbnd, eastbnd


# In[ ]:


def get_ioos_association_url(ioos_assoc):
    
    """
    This function is used to return the URL
    for the defined IOOS Regional Association (RA)
    
    Function inputs:
    ioos_assoc - acronym of the specified IOOS RA
    
    Function outputs:
    ioos_url - URL for the specified IOOS RA
    """
    
    ioos_ra_urls = {
        'AOOS': 'https://aoos.org/',
        'NANOOS': 'https://www.nanoos.org/',
        'CENCOOS': 'https://cencoos.org/',
        'SCCOOS': 'https://sccoos.org/',
        'PacIOOS': 'https://www.pacioos.hawaii.edu/',
        'GLOS': 'https://glos.org/',
        'GCOOS': 'https://gcoos.org/',
        'NERACOOS': 'https://neracoos.org/',
        'MARACOOS': 'https://maracoos.org/',
        'SECOORA': 'https://secoora.org/',
        'CariCOOS': 'https://www.caricoos.org/' 
    }
    
    ioos_ra_keys = ioos_ra_urls.keys()
    if any([ii == ioos_assoc for ii in ioos_ra_keys]):
        return ioos_ra_urls[ioos_assoc]
    else:
        return None


# In[ ]:


def make_metadata_json(basedir, bb_loc, loc_meta, spotters_dict, rebuildFlag=False):
    
    """
    This function creates a metadata json for a given
    location. It creates a dictionary of the fields
    necessary to fill in the metadata for the
    netCDFs for each dataset, and writes it out
    to a json file.
    
    Function inputs:
    basedir  - the base directory path of all datafiles
    bb_loc   - the location ID for a given Backyard Buoys dataset
    loc_meta - the metadata associated with a specific location
    spotters_dict - a dictionary containing the metadata related
                    to every spotter that passes through the
                    specified location
    """

    ######################################
    # Build the creator/contributor fields
    
    # Initialize the strings for all the fields
    creator_name = ''
    creator_email = ''
    creator_institution = ''
    creator_url = ''
    creator_type = ''
    creator_spotterid = ''
    contributor_names = ''
    contributor_role = ''
    contributor_url = ''
    
    # Initialize empty lists to track the unique
    # creator and contributor names
    unique_creator_names = []
    unique_contributor_names = []
    
    # Loop through each spotter given in the spotter_dict
    # and extract out the creator and contributors for
    # this spotter
    for spotter_id in spotters_dict.keys():
        # Based upon the strings, determine what sort of
        # spacing to use when appending on extra data
        if creator_name == '':
            space_text = ''
        else:
            space_text = ', '
            
        # If the creator name is not in the list of unique
        # creator names, append on data for that creator
        if not(any([name == spotters_dict[spotter_id]['owner'] 
                    for name in unique_creator_names])):
            unique_creator_names.append(spotters_dict[spotter_id]['owner'])
            creator_name += space_text + spotters_dict[spotter_id]['owner']
            creator_email += space_text + spotters_dict[spotter_id]['contact_email']
            creator_institution += space_text + spotters_dict[spotter_id]['owner_org']
            creator_url += space_text + spotters_dict[spotter_id]['org_website']
            creator_type += space_text + spotters_dict[spotter_id]['org_sector']
        
        creator_spotterid += space_text + spotter_id
        
        # If any data is given in the acknowledgements for a given spotter,
        # parse out the contributor fields for that spotter buoy
        if spotters_dict[spotter_id]['acknowledgements'] != '':
            [new_contributor_names, new_contributor_roles, 
             new_contributor_urls, 
             unique_contributor_names] = bb_api_build_contributors(spotters_dict[spotter_id]['acknowledgements'],
                                                                  unique_contributor_names)
            contributor_names += space_text + new_contributor_names
            contributor_role += space_text + new_contributor_roles
            contributor_url += space_text + new_contributor_urls
    
    # If after looping through all the spotters, the contributor_names
    # are still empty, create default blank values
    if contributor_names == '':
        contributor_names = '--'
        contributor_role = None
        contributor_url = None


    ############################################
    # Build a dictionary containing the metadata
    # for a given location
    loc_metadata = {
        "location_name": loc_meta['label'],
        "location_id": loc_meta['loc_id'],  
        "ioos_association": loc_meta['ioos_ra'],
        "ioos_url": get_ioos_association_url(loc_meta['ioos_ra']),
        "wmo_code": '',
        "region": loc_meta['region'],
        "northern_bound": loc_meta['lat_n'],
        "southern_bound": loc_meta['lat_s'],
        "western_bound": loc_meta['lon_w'],
        "eastern_bound": loc_meta['lon_e'],
        "creator_name": creator_name,
        "creator_email": creator_email,
        "creator_institution": creator_institution,
        "creator_url": creator_url,
        "creator_type": creator_type,
        "creator_spotter_ids": creator_spotterid,
        "contributor_name": contributor_names,
        "contributor_role": contributor_role,
        "contributor_url": contributor_url
    }

    # Create a data dictionary, and fill it with
    # the creation data for the current json, and
    # the metadata for this location
    datadict = {
        "creation_date": datetime.datetime.now().strftime('%Y-%b-%dT%H:%M:%S'),
        "metadata": loc_metadata
    }
    
    ####################################
    # Write out the metadata to a json
    
    # If a data directory does not exist for a given
    # location, make a data directory
    sourcedir = os.path.join(basedir, bb_loc)
    if not(os.path.exists(sourcedir)):
        os.mkdir(sourcedir)
    
    # If a metadata directory does not exist in the
    # folder for a given location, make a metadata directory
    sourcedir = os.path.join(sourcedir, 'metadata')
    if not(os.path.exists(sourcedir)):
        os.mkdir(sourcedir)
    
    # Construct the metadata json filepath, and
    # create a defualt flag value, indicating to 
    # write out the json file
    filename = bb_loc + '_metadata.json'
    filepath = os.path.join(sourcedir, filename)
    make_json = True
    
    # Check if a metadata json file already exists, and
    # if it does exist, check if there are any changes in
    # for the new json
    if os.path.exists(filepath):
        # Check if a metadata archive directory exists,
        # and if not, make a metadata archive directory
        if not(os.path.exists(os.path.join(sourcedir, 'archive'))):
            os.mkdir(os.path.join(sourcedir, 'archive'))
            
        # Load in the existing metadata json
        with open(filepath, 'r') as meta_json:
            check_json = json.load(meta_json)
        
        # Compare the metadata in the new data dictionary
        # to that loaded in from the existing metadata json
        if datadict['metadata'] != check_json['metadata']: 
            # If the metadata has changed from the existing json,
            # then archive the existing json, and move it into
            # the archive directory
            archive_name = (bb_loc + '_metadata_' + 
                            datetime.datetime.now().strftime('%Y%m%d') + '.json')
            shutil.move(filepath, os.path.join(sourcedir, 'archive', archive_name))
        else:
            # If the metadata json has not changed, then
            # there is no need to create a new json.
            # Change the make_json flag to false
            make_json = False
    
    # If the make_json flag is set to true
    # (i.e., there is not an existing metadata json,
    #  or there has been a change to the metadata json),
    # then write out a new file
    if make_json:
        with open(filepath, 'w') as bb_json:
            json.dump(datadict, bb_json)
        
    return


# def make_project_metadata(loc_id, addSpotterFlag=False):
#     
#     ##################################################
#     # Load in the metadata from the Google sheets,
#     # and all the buoys in the Backyard Buoys data API
#     basedir = bb.get_datadir()
#     bb_locs = bb_da.bbapi_get_locations() 
#     bb_plats = bb_da.bbapi_get_platforms()
#     bb_plats_ret = bb_da.bbapi_get_platforms(retiredFlag=True)
#     for plat in bb_plats_ret.keys():
#         bb_plats[plat] = bb_plats_ret[plat]
# 
#     qc_df = get_all_google_qcdata()
#     
# 
#     # Check if the location ID is present in valid locations
#     if not(any([loc == loc_id for loc in bb_locs.keys()])):
#         print('No data found which matches the location ID: ' + loc_id)
#         print('Unable to proceed.')
#         return False
#     else:
#         loc_meta = bb_locs[loc_id]
#         
#     # Double-check that the location ID
#     # is a Backyard Buoys site
#     if not(loc_meta['is_byb'] == 'yes'):
#         print('Location is not a Backyard Buoys site.')
#         print('Do not create metadata for location ID: ' + loc_id)
#         return False
#     
# 
#     # Use the location ID to pull a subset of all of the data, and to 
#     # find the unique spotter IDs associated with that location ID
#     locdata = bb_da.bbapi_get_location_data(loc_id, 
#                                             vars_to_get='WaveHeightSig',
#                                             time_start=datetime.datetime(2022,6,1).strftime('%Y-%m-%dT%H:%M:%SZ'))
#     if locdata is None:
#         print('No spotter buoy data is found for this location ID.')
#         print('Do not create metadata for location ID: ' + loc_id)
#         return False
#     spotter_ids = np.unique(locdata['WaveHeightSig']['data']['platform_id'])
#     spotters_dict = {}
#     for spotter_id in spotter_ids:
#         if spotter_id not in bb_plats.keys():
#             print('No spotter metadata has been added for spotter ' + spotter_id)
#             print('Unable to add data. Continue on.')
#             continue
#         spotters_dict[spotter_id] = bb_plats[spotter_id]
#     if len(spotters_dict) == 0:
#         print('No spotter metadata has been added.')
#         print('Do not create metadata for location ID: ' + loc_id)
#         return False
# 
#     # Make the metadata and QC jsons for the specified project
#     make_metadata_json(basedir, loc_id, 
#                        loc_meta, spotters_dict)
#     make_qcdata_json(basedir, loc_id, qc_df)
#     make_location_info_json(basedir, loc_id)
#     
#     return True

# # Location Info

# In[ ]:


def make_location_info_json(basedir, loc_id, rebuildFlag=False):
    
    """
    Location info
    """
    
    
    # Define the info json path
    sourcedir = os.path.join(basedir, loc_id, 'metadata')
    infodir = os.path.join(sourcedir, loc_id +'_info.json')

    
    # If the info path already exists, 
    # load in the info json, and update the relevant fields
    if os.path.exists(infodir) and not(rebuildFlag):
        
        with open(infodir, 'r') as info_json:
            infodict = json.load(info_json)
            
        # Extract out a list of all spotter IDs for a location
        if infodict['spotter_ids'] == '':
            spotter_list = []
        elif ',' in infodict['spotter_ids']:
            spotter_list = [ii.strip() for ii in 
                            np.unique(infodict['spotter_ids'].split(','))]
        else:
            spotter_list = [infodict['spotter_ids']]
           
        
        # Load in the location info
        bb_locs = bb_da.bbapi_get_locations(recentFlag=True)
        addspotterFlag = False
        if not(any([loc == loc_id for loc in bb_locs.keys()])) and not(rebuildFlag):
            print('No recent data for location ID: ' + loc_id)
            print('Do not update location info.')
            return False
        
        try:
            # If there is recent data (i.e., less than 30 days old)
            # then the API call will have that data, which can be extracted
            loc_info = bb_locs[loc_id]
            loc_data = loc_info['data']
            del loc_info['data']

            # From the most recent data,
            # get the timestamp of the wave height data
            wave_index = next((index for (index, d) in enumerate(loc_data) 
                               if d["var_id"] == "WaveHeightSig"), None)
            ref_date = datetime.datetime(1970,1,1)
            wavedate = (ref_date + 
                        datetime.timedelta(seconds=loc_data[wave_index]['timestamp']))

            # Get the spotter ID from the most recent data
            new_spotter = loc_data[wave_index]['platform_id']
            new_spotter_list = spotter_list
            if not(any([new_spotter == spotter 
                        for spotter in spotter_list])):
                addspotterFlag = True
                new_spotter_list.append(new_spotter)
            
        except:
            
            # If there is not recent data for that project,
            # accessing that data from the API will not return
            # anything.
            #
            #
            if (datetime.datetime.strptime(infodict['recent_date'], '%Y-%m-%dT%H:%M:%SZ') <
                datetime.datetime.now() - datetime.timedelta(days=30)):
                
                print('No recent data: do not update location info json.')
                return False
            else:
                bb_locs = bb_da.bbapi_get_locations()
                loc_info = bb_locs[loc_id]

                all_locdata = bb_da.bbapi_get_location_data(loc_id, 
                                              vars_to_get='WaveHeightSig',
                                              time_start=datetime.datetime(2022,6,1).strftime('%Y-%m-%dT%H:%M:%SZ'))
                if all_locdata is None:
                    print('No data was found at this location!')
                    print('Do not make any updates to the location info json.')
                    return False
                
                # Get the timestamp of the wave height data
                max_timestamp = max(all_locdata['WaveHeightSig']['data']['timestamp'])
                ref_date = datetime.datetime(1970,1,1)
                wavedate = (ref_date + 
                            datetime.timedelta(seconds=max_timestamp))
            
                # Get the spotter ID from the most recent data
                new_spotter = all_locdata['WaveHeightSig']['data']['platform_id'][0][0]
                new_spotter_list = spotter_list
                if not(any([new_spotter == spotter 
                            for spotter in spotter_list])):
                    addspotterFlag = True
                    new_spotter_list.append(new_spotter)
                        
        
        # Get spotter platform data        
        bb_plats = bb_da.bbapi_get_platforms()
        bb_plats_ret = bb_da.bbapi_get_platforms(retiredFlag=True)
        for plat in bb_plats_ret.keys():
            bb_plats[plat] = bb_plats_ret[plat]
        spotter_liststr = ''
        spotter_addlist = [ii for ii in spotter_list]
        spotter_data = {}
        for spotter in new_spotter_list:
            if spotter_liststr == '':
                extra_str = ''
            else:
                extra_str = ', '
            if (spotter not in bb_plats.keys()) or (any([spotter == ii for ii in spotter_addlist])):
                continue
            else:
                spotter_addlist.append(spotter)
                spotter_liststr += extra_str + spotter
                spotter_data[spotter] = bb_plats[spotter]
        infodict['spotter_ids'] = spotter_liststr
        infodict['spotter_data'] = spotter_data
                
        activeFlag = False
        if loc_info['status'] == 'active':
            activeFlag = True
        
            
        # Update the date of recent data
        infodict['recent_date'] = wavedate.strftime('%Y-%m-%dT%H:%M:%SZ')
        infodict['active'] = activeFlag
        
        if addspotterFlag:
            infodict['spotter_ids'] = (infodict['spotter_ids'] + ', ' + new_spotter)
            
        # Check the location histories, and check if they have changed
        history_dates = [datetime.datetime.strptime(ii, '%Y-%m-%dT%H:%M:%SZ')
                            for ii in infodict['loc_history'].keys()]
        latest_date = max(history_dates)
        latest_history = infodict['loc_history'][latest_date.strftime('%Y-%m-%dT%H:%M:%SZ')]
        
        # If the dictiorary of location info for the most
        # recent data is not the same as the info for the
        # latest history, then append on a new location info,
        # using the date of the most recent data as the entry
        if latest_history != loc_info:
            print('Add new location history information: ' + wavedate.strftime('%Y-%m-%dT%H:%M:%SZ'))
            infodict['loc_history'][wavedate.strftime('%Y-%m-%dT%H:%M:%SZ')] = loc_info
        
    else:
        
        bb_locs = bb_da.bbapi_get_locations()
        if not(any([loc == loc_id for loc in bb_locs.keys()])):
            print('No recent data for location ID: ' + loc_id)
            print('Do not update location info.')
            return False
        else:
            loc_info = bb_locs[loc_id]
        
        all_locdata = bb_da.bbapi_get_location_data(loc_id, 
                                      vars_to_get='WaveHeightSig',
                                      time_start=datetime.datetime(2022,6,1).strftime('%Y-%m-%dT%H:%M:%SZ'))
        if all_locdata is None:
            print('No data was found at this location!')
            print('Do not make any updates to the location info json.')
            return False
                
        ref_date = datetime.datetime(1970,1,1)
        startdate = (ref_date + 
                     datetime.timedelta(seconds=min(all_locdata['WaveHeightSig']['data']['timestamp'])))
        wavedate = (ref_date + 
                     datetime.timedelta(seconds=max(all_locdata['WaveHeightSig']['data']['timestamp'])))
        
        activeFlag = False
        if loc_info['status'] == 'active':
            activeFlag = True
            
        addspotterFlag = True
        
        # Otherwise, create a new info dictionary
        # with relevant info about the location
        spotter_list = np.unique(all_locdata['WaveHeightSig']['data']['platform_id'][0])
        bb_plats = bb_da.bbapi_get_platforms(allplatsFlag=True)
        spotter_liststr = ''
        spotter_addlist = []
        spotter_data = {}
        for spotter in spotter_list:
            if spotter_liststr == '':
                extra_str = ''
            else:
                extra_str = ', '
            if (spotter not in bb_plats.keys()) or (any([spotter == ii for ii in spotter_addlist])):
                continue
            else:
                spotter_addlist.append(spotter)
                spotter_liststr += extra_str + spotter
                spotter_data[spotter] = bb_plats[spotter]
                
        infodict =  {
            'location_id': loc_id,
            'label': loc_info['label'],
            'ioos_ra': loc_info['ioos_ra'],
            'region': loc_info['region'],
            'start_date': startdate.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'recent_date': wavedate.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'active': activeFlag,
            'spotter_ids': spotter_liststr,
            'loc_history': {startdate.strftime('%Y-%m-%dT%H:%M:%SZ'): loc_info},
            'spotter_data': spotter_data
        }
    
    
    # Write the info data as a json
    make_json = True
    if os.path.exists(infodir):
        if not(os.path.exists(os.path.join(sourcedir, 'archive'))):
            os.mkdir(os.path.join(sourcedir, 'archive'))
            
        with open(infodir,'r') as info_json:
            check_json = json.load(info_json)
            
        if ((infodict['loc_history'] != check_json['loc_history']) 
            or
            (('spotter_data' in check_json.keys())
             and
             (infodict['spotter_data'] != check_json['spotter_data']))
            or
            ('spotter_data' not in check_json.keys())
           ):
            archive_name = (loc_id + '_info_' + 
                            datetime.datetime.now().strftime('%Y%m%d') + '.json')
            shutil.move(infodir, os.path.join(sourcedir, 'archive', archive_name))
        else:
            make_json = False
            
    if make_json:
        with open(infodir, 'w') as info_json:
            json.dump(infodict, info_json)
        
    return addspotterFlag


# # QARTOD Limits Compilation functions

# In[ ]:


def get_all_google_qcdata():
    
    google_info = bb.load_googleinfo_json()
    qcdata_sheetid = google_info['qartod']
    all_qc = batch_get_values(qcdata_sheetid,'A1:CW')

    qc_df = pd.DataFrame(columns = all_qc['valueRanges'][0]['values'][0],
                         data=all_qc['valueRanges'][0]['values'][1:])
    
    return qc_df


# In[ ]:


def make_qcdata_json(basedir, bb_loc, qc_df=None, rebuildFlag=False):
    
    if qc_df is None:
        qc_df = get_all_google_qcdata()
    
    # Extract out the QC limits corresponding to the correct project.
    # If no corresponding project is found, use the default limits.
    if any(qc_df['loc_id'] == bb_loc):
        qc_ind = np.where(qc_df['loc_id'] == bb_loc)[0][0]
        default_flag = False
    else:
        qc_ind = np.where(qc_df['loc_id'] == 'default')[0][0]
        default_flag = True
    qc_lims = qc_df.iloc[qc_ind]
    
    
    # Create a dictionary to hold all the QC limits
    qc_dict = {}
    for lim in qc_lims.index[1:]:
        qc_dict[lim] = qc_lims[lim]
        
        
    # Build a total dictionary for the QC json
    datadict = {
        "creation_date": datetime.datetime.now().strftime('%Y-%b-%dT%H:%M:%S'),
        "default_limits_used": default_flag,
        "qartod_limits": qc_dict
    }
    
    
    # Write out the QC json
    
    sourcedir = os.path.join(basedir, bb_loc)
    if not(os.path.exists(sourcedir)):
        os.mkdir(sourcedir)
        
    sourcedir = os.path.join(sourcedir, 'metadata')
    if not(os.path.exists(sourcedir)):
        os.mkdir(sourcedir)
        
    filename = bb_loc + '_qartod.json'
    filepath = os.path.join(sourcedir, filename)
    
    make_json = True
    if os.path.exists(filepath):
        if not(os.path.exists(os.path.join(sourcedir, 'archive'))):
            os.mkdir(os.path.join(sourcedir, 'archive'))
            
        with open(filepath, 'r') as qartod_json:
            check_json = json.load(qartod_json)
            
        if datadict['qartod_limits'] != check_json['qartod_limits']:
            archive_name = bb_loc + '_qartod_' + datetime.datetime.now().strftime('%Y%m%d') + '.json'
            shutil.move(filepath, os.path.join(sourcedir, 'archive', archive_name))
        else:
            make_json = False
            
    if make_json:
        with open(filepath, 'w') as bb_json:
            json.dump(datadict, bb_json)
        
    return


# # General metadata wrapper function

# In[ ]:


def make_projects_metadata(loc_ids=None, rebuild_flag=False):
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations() 
    bb_plats = bb_da.bbapi_get_platforms(allplatsFlag=True)
    qc_df = get_all_google_qcdata()
    
    # Filter out non-official Backyard Buoys sites
    # Only process locations where is_byb='yes' (excludes "Friends of..." sites)
    locs_to_del = []  # Track locations to remove
    for loc in bb_locs:
        # Check if this is an official Backyard Buoys site
        if not(bb_locs[loc]['is_byb'] == 'yes'):
            locs_to_del.append(loc)  # Mark for deletion
    
    # Remove non-official locations from the dictionary
    for loc in locs_to_del:
        del bb_locs[loc]
        
    
    
    ##############################
    # Loop through all indices
    
    
    # Track which locations successfully had metadata created
    new_metadata_locs = []
    
    # Normalize loc_ids to always be a list for iteration
    if loc_ids is None:
        # No specific locations requested - process all official locations
        loc_ids = list(bb_locs.keys())
    elif not(isinstance(loc_ids, list)):
        # Single location provided as string - convert to list
        # This ensures consistent iteration logic below
        loc_ids = [loc_ids]
    
    print('\n\n\nLoop through all indices:')
    for loc_id in loc_ids:
        
        print('\nLocation ID:',loc_id)
        
        # Check if the location ID is present in valid locations
        if not(any([loc == loc_id for loc in bb_locs.keys()])):
            print('No data found which matches the location ID: ' + loc_id)
            print('Unable to proceed.')
            continue
        else:
            loc_meta = bb_locs[loc_id]
        
        # Double-check that the location ID
        # is a Backyard Buoys site
        if not(loc_meta['is_byb'] == 'yes'):
            print('Location is not a Backyard Buoys site.')
            print('Do not create metadata for location ID: ' + loc_id)
            continue


        # Use the location ID to pull a subset of all of the data, and to 
        # find the unique spotter IDs associated with that location ID
        locdata = bb_da.bbapi_get_location_data(loc_id, 
                                                vars_to_get='WaveHeightSig',
                                                time_start=datetime.datetime(2022,6,1).strftime('%Y-%m-%dT%H:%M:%SZ'))
        if locdata is None:
            print('No spotter buoy data is found for this location ID.')
            print('Do not create metadata for location ID: ' + loc_id)
            continue
        # Extract unique spotter IDs from the wave height data
        # np.atleast_1d ensures we can iterate even with a single spotter
        spotter_ids = np.atleast_1d(np.unique(locdata['WaveHeightSig']['data']['platform_id']))
        
        # Build dictionary of spotter metadata for all spotters at this location
        spotters_dict = {}
        for spotter_id in spotter_ids:
            # Verify that metadata exists for this spotter
            if spotter_id not in bb_plats.keys():
                print('No spotter metadata has been added for spotter ' + spotter_id)
                print('Unable to add data. Continue on.')
                continue
            # Add this spotter's metadata to the dictionary
            spotters_dict[spotter_id] = bb_plats[spotter_id]
        if len(spotters_dict) == 0:
            print('No spotter metadata has been added.')
            print('Do not create metadata for location ID: ' + loc_id)
            continue

        # Make the metadata and QC jsons for the specified project
        make_metadata_json(basedir, loc_id, 
                           loc_meta, spotters_dict, rebuild_flag)
        make_qcdata_json(basedir, loc_id, qc_df, rebuild_flag)
        make_location_info_json(basedir, loc_id, rebuild_flag)
        
        # Append on the new metadata
        new_metadata_locs.append(loc_id)
        
    if len(new_metadata_locs) == 0:
        new_metadata_locs = None

    return new_metadata_locs

