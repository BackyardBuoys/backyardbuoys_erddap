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


# # Google API Functions

# In[ ]:


def get_auth_dir():
    
    if '/home/stravis' in os.getcwd():
        auth_dir = '/home/stravis/backyardbuoys_files/tokens/auth_token'
    else:
        auth_dir = 'C:/Users/APLUser/NANOOS/Backyard_Buoys/auth_token'
        
    return auth_dir


# In[ ]:


def get_user_token():
    
    """
    Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
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


# # Metadata compilation functions

# In[ ]:


def get_all_metadata():
    
    google_info = bb.load_googleinfo_json()
    metadata_sheetid = google_info['metadata']
    all_meta = batch_get_values(metadata_sheetid,'A1:N')

    meta_df = pd.DataFrame(columns = all_meta['valueRanges'][0]['values'][0],
                           data=all_meta['valueRanges'][0]['values'][1:])
    
    return meta_df


# In[ ]:


def get_contributors(contributors):
    
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


def make_metadata_json(basedir, meta_series, bb_lab, bb_loc):

    [contributor_names, contributor_roles, 
     contributor_urls] = get_contributors(meta_series['Contributor_fields'])

    northbnd, southbnd, westbnd, eastbnd = get_lat_lon_bounds(meta_series)

    loc_metadata = {
        "location_name": bb_lab,
        "location_id": bb_loc,
        "creator_name": meta_series['Owner Name'],
        "creator_email": meta_series['Owner Email'],
        "creator_institution": meta_series['Owner Organization'],
        "creator_url": meta_series['Owner URL'],
        "creator_type": meta_series['Owner Sector'],
        "contributor_name": contributor_names,
        "contributor_role": contributor_roles,
        "contributor_url": contributor_urls,    
        "ioos_association": meta_series['IOOS_association'],
        "ioos_url": get_ioos_association_url(meta_series['IOOS_association']),
        "wmo_code": meta_series['WMO_Code'],
        "northern_bound": northbnd,
        "southern_bound": southbnd,
        "western_bound": westbnd,
        "eastern_bound": eastbnd,
    }
    
    datadict = {
        "creation_date": datetime.datetime.now().strftime('%Y-%b-%dT%H:%M:%S'),
        "metadata": loc_metadata
    }

    sourcedir = os.path.join(basedir, bb_loc)
    if not(os.path.exists(sourcedir)):
        os.mkdir(sourcedir)
        
    sourcedir = os.path.join(sourcedir, 'metadata')
    if not(os.path.exists(sourcedir)):
        os.mkdir(sourcedir)
        
    filename = bb_loc + '_metadata.json'
    filepath = os.path.join(sourcedir, filename)
    
    if os.path.exists(filepath):
        if not(os.path.exists(os.path.join(sourcedir, 'archive'))):
            os.mkdir(os.path.join(sourcedir, 'archive'))
        
        archive_name = bb_loc + '_metadata_' + datetime.datetime.now().strftime('%Y%m%d') + '.json'
        shutil.move(filepath, os.path.join(sourcedir, 'archive', archive_name))
            
    
    with open(filepath, 'w') as bb_json:
        json.dump(datadict, bb_json)
        
    return


# In[ ]:


def make_project_metadata(loc_id):
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations() 
    meta_df = get_all_metadata()
    qc_df = get_all_qcdata()
    
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
    make_metadata_json(basedir, meta_df.iloc[meta_ind], 
                       bb_loc_labels[bb_ind], loc_id)
    make_qcdata_json(basedir, loc_id, qc_df)
    
    return True


# In[ ]:


def make_projects_metadata(loc_ids):
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations() 
    meta_df = get_all_metadata()
    qc_df = get_all_qcdata()
    
    # Find the indice of the matching location ID
    bb_loc_ids= [bb_locs[ii]['loc_id'] for ii in bb_locs.keys()]
    bb_loc_labels = [bb_locs[ii]['label'] for ii in bb_locs.keys()]
    
    # Step through every instance of loc_id, and try to make metadata
    if not(isinstance(loc_ids,list)):
        loc_ids = [loc_ids]
    
    new_metadata_locs = []
    for loc_id in loc_ids:
        if any([ii == loc_id for ii in bb_loc_ids]):
            bb_ind = np.where([ii == loc_id for ii in bb_loc_ids])[0][0]
        else:
            print('No data found which matches the location ID: ' + loc_id)
            print('Unable to proceed.')
            continue

        # Get the metadata for the specific project
        bb_label = bb_loc_labels[bb_ind]
        if any(meta_df['Location Name'] == bb_label):
            meta_ind = np.where(meta_df['Location Name'] == bb_label)[0][0]
        else:
            print('No metadata was found which matched the location ID: ' + loc_id)
            print('Unable to proceed.')
            continue


        # Make the metadata and QC jsons for the specified project
        make_metadata_json(basedir, meta_df.iloc[meta_ind], 
                           bb_loc_labels[bb_ind], loc_id)
        make_qcdata_json(basedir, loc_id, qc_df)
        new_metadata_locs.append(loc_id)
        
    if len(new_metadata_locs) == 0:
        new_metadata_locs = None

    return new_metadata_locs


# In[ ]:


def make_all_metadata():
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    meta_df = get_all_metadata()
    bb_locs = bb_da.bbapi_get_locations() 
    qc_df = get_all_qcdata()
    
    
    #############################################
    # Find the projects that have data in the API
    # and have metadata
    
    # Extract out location labels and IDs from the Backyard Buoys API data
    bb_loc_ids= [bb_locs[ii]['loc_id'] for ii in bb_locs.keys()]
    bb_loc_labels = [bb_locs[ii]['label'] for ii in bb_locs.keys()]

    # Find the matching metadata index, if available, for each project
    meta_inds = []
    for ii in range(0,len(bb_loc_labels)):

        bb_label = bb_loc_labels[ii]
        bb_loc_id = bb_loc_ids[ii]

        if any(meta_df['Location Name'] == bb_label):
            meta_inds.append(np.where(meta_df['Location Name'] == bb_label)[0][0])
        else:
            meta_inds.append(None)
            
            
    ##################################
    # Step through each project and
    # make a metadata json for that project
    for ii in range(0,len(meta_inds)):

        if meta_inds[ii] is None:
            continue

        make_metadata_json(basedir, meta_df.iloc[meta_inds[ii]], 
                              bb_loc_labels[ii], bb_loc_ids[ii])
        make_qcdata_json(basedir, bb_loc_ids[ii], qc_df)
        
        
    return


# # QARTOD Limits Compilation functions

# In[ ]:


def get_all_qcdata():
    
    google_info = bb.load_googleinfo_json()
    qcdata_sheetid = google_info['qartod']
    all_qc = batch_get_values(qcdata_sheetid,'A1:CW')

    qc_df = pd.DataFrame(columns = all_qc['valueRanges'][0]['values'][0],
                         data=all_qc['valueRanges'][0]['values'][1:])
    
    return qc_df


# In[ ]:


def make_qcdata_json(basedir, bb_loc, qc_df=None):
    
    if qc_df is None:
        qc_df = get_all_qcdata()
    
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
    
    if os.path.exists(filepath):
        if not(os.path.exists(os.path.join(sourcedir, 'archive'))):
            os.mkdir(os.path.join(sourcedir, 'archive'))
        
        archive_name = bb_loc + '_qartod_' + datetime.datetime.now().strftime('%Y%m%d') + '.json'
        shutil.move(filepath, os.path.join(sourcedir, 'archive', archive_name))
            
    
    with open(filepath, 'w') as bb_json:
        json.dump(datadict, bb_json)
        
    return


# In[ ]:




