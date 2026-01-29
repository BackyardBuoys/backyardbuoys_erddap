#!/usr/bin/env python
# coding: utf-8

"""
BackyardBuoys ERDDAP - General Utility Functions
=================================================

This module provides utility functions used across the BackyardBuoys ERDDAP
system. Functions include path management, configuration loading, and metadata
access.

Key Functions:
    - get_datadir() : Returns base data directory path
    - get_location_metadata() : Loads location metadata from JSON
    - load_googleinfo_json() : Loads Google Sheets configuration
    - load_bbapi_info_json() : Loads API endpoint configuration

Author: Seth Travis
Organization: Backyard Buoys
"""

import os
import json


def get_location_metadata(loc_id):
    """
    Load metadata JSON file for a specific location.
    
    Retrieves the location-specific metadata from the JSON file stored in
    the location's metadata directory. This metadata includes information
    about the data owner, geographic bounds, IOOS association, and other
    dataset attributes.
    
    Parameters
    ----------
    loc_id : str
        Location identifier (e.g., 'quileute_south', 'gambell')
    
    Returns
    -------
    dict or None
        Metadata dictionary containing location information, or None if
        metadata file does not exist.
        
        Metadata structure:
        {
            'location_name': str,
            'location_id': str,
            'creator_name': str,
            'creator_email': str,
            'creator_institution': str,
            'creator_url': str,
            'creator_type': str,
            'contributor_name': str,
            'contributor_role': str,
            'contributor_url': str,
            'ioos_association': str,
            'ioos_url': str,
            'wmo_code': str,
            'northern_bound': str,
            'southern_bound': str,
            'western_bound': str,
            'eastern_bound': str
        }
    
    Examples
    --------
    >>> meta = get_location_metadata('quileute_south')
    >>> if meta:
    ...     print(f"Location: {meta['location_name']}")
    ...     print(f"Owner: {meta['creator_institution']}")
    Location: Quileute South
    Owner: Quileute Indian Tribe
    
    >>> # Check if metadata exists before processing
    >>> if get_location_metadata('new_location') is None:
    ...     print("Metadata does not exist for this location")
    Metadata does not exist for this location
    
    Notes
    -----
    Expected file path: {basedir}/{loc_id}/metadata/{loc_id}_metadata.json
    
    The metadata JSON is created by backyardbuoys_build_metadata.py from
    Google Sheets data.
    
    See Also
    --------
    backyardbuoys_build_metadata.make_metadata_json : Creates metadata JSON
    """
    
    # Get the base directory where all location data is stored
    basedir = get_datadir()
    
    # Construct the path to the metadata JSON file
    # Path format: {basedir}/{loc_id}/metadata/{loc_id}_metadata.json
    pathdir = os.path.join(basedir, loc_id, 'metadata', loc_id+'_metadata.json')
    
    # Check if the metadata file exists
    if not(os.path.exists(pathdir)):
        # Metadata file not found - return None
        return None
    else:
        # Open and load the metadata JSON file
        with open(pathdir) as meta_json:
            meta = json.load(meta_json)
    
    # Return only the 'metadata' portion of the JSON
    # (The JSON also contains 'creation_date' at the top level)
    return meta['metadata']

def get_location_info(loc_id):
    
    """
    This function identifies the directory
    that contains the info jsons for a given
    location ID, and loads in that json file.
    
    Function inputs:
    loc_id - the location ID, taken from the Backyard
             Buoys API, for a given dataset
             
    Function outputs:
    loc_info - the location information for a given location ID,
               loaded in from the dataset info json
    """
    
    import os
    import json
    
    # Construct the path to the location info JSON file
    basedir = get_datadir()
    pathdir = os.path.join(basedir, loc_id, 'metadata', loc_id+'_info.json')
    
    # Check if the info file exists
    if not(os.path.exists(pathdir)):
        return None  # Info file not found
    else:
        # Load the location info JSON
        with open(pathdir) as info_json:
            loc_info = json.load(info_json)
    
        
    return loc_info

def get_datadir():
    """
    Get the base directory path for all data files.
    
    Returns the appropriate data directory path based on the current working
    directory. Uses different paths for production (Linux server) and
    development (Windows) environments.
    
    Returns
    -------
    str
        Absolute path to the base data directory
    
    Examples
    --------
    >>> datadir = get_datadir()
    >>> print(datadir)
    /data/tomcat/dataset_files/
    
    >>> # Use to construct location-specific paths
    >>> loc_id = 'quileute_south'
    >>> loc_path = os.path.join(get_datadir(), loc_id)
    >>> print(loc_path)
    /data/tomcat/dataset_files/quileute_south/
    
    Notes
    -----
    Directory structure:
        {datadir}/
        ├── {location_id}/
        │   ├── bb_{location_id}_{year}.nc
        │   └── metadata/
        │       ├── {location_id}_metadata.json
        │       └── {location_id}_qartod.json
        └── ...
    
    Environment Detection:
        - Production (Linux): /home/stravis/ in current working directory
        - Development (Windows): Otherwise
    
    See Also
    --------
    get_location_metadata : Load metadata for a location
    """
    
    # Get the current working directory
    basedir = os.path.dirname(__file__)
    
    # Load in the directory info json
    with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the erddap data
    basedir = dir_info['erddap_data']
        
    return basedir


def load_googleinfo_json():
    """
    Load Google Sheets configuration information.
    
    Loads the JSON configuration file containing Google Sheets IDs for
    metadata and QARTOD limits. This file is used to access the Google
    Sheets that store location metadata and quality control parameters.
    
    Returns
    -------
    dict
        Configuration dictionary with Google Sheet IDs.
        
        Expected structure:
        {
            'metadata': str,  # Sheet ID for location metadata
            'qartod': str     # Sheet ID for QC limits
        }
    
    Examples
    --------
    >>> config = load_googleinfo_json()
    >>> metadata_sheet_id = config['metadata']
    >>> qartod_sheet_id = config['qartod']
    
    >>> # Use in metadata retrieval
    >>> from googleapiclient.discovery import build
    >>> service = build('sheets', 'v4', credentials=creds)
    >>> result = service.spreadsheets().values().get(
    ...     spreadsheetId=config['metadata'],
    ...     range='A1:N'
    ... ).execute()
    
    Notes
    -----
    File location:
        - Production: /home/stravis/backyardbuoys_files/python_scripts/info_jsons/
        - Development: C:/Users/APLUser/NANOOS/Backyard_Buoys/python_scripts/info_jsons/
    
    The Google Sheets contain:
        - Metadata sheet: Location information, creator details, geographic bounds
        - QARTOD sheet: Quality control test limits for all variables
    
    See Also
    --------
    backyardbuoys_build_metadata.get_all_metadata : Retrieve metadata from sheets
    backyardbuoys_build_metadata.get_all_qcdata : Retrieve QC limits from sheets
    """
    
    # Get the current working directory to determine environment
    basedir = os.path.dirname(__file__)
    
    # Load in the directory info json
    with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the erddap data
    infodir = dir_info['info_jsons']
    
    # Open and load the Google Sheets configuration JSON file
    with open(os.path.join(infodir, 'google_info.json'), 'r') as f:
        googleinfo = json.load(f)
    
    return googleinfo


def load_bbapi_info_json():
    """
    Load Backyard Buoys API endpoint configuration.
    
    Loads the JSON configuration file containing URLs for the Backyard Buoys
    data API endpoints. These endpoints are used to retrieve location data,
    platform information, and time-series measurements.
    
    Returns
    -------
    dict
        Configuration dictionary with API endpoint URLs.
        
        Expected structure:
        {
            'get_locations': str,      # URL for locations list
            'get_location_data': str,  # URL for location data
            'get_platform_data': str   # URL for platform data
        }
    
    Examples
    --------
    >>> api_config = load_bbapi_info_json()
    >>> locations_url = api_config['get_locations']
    >>> print(locations_url)
    https://data.backyardbuoys.org/get_locations
    
    >>> # Use in API requests
    >>> import requests
    >>> response = requests.get(api_config['get_locations'])
    >>> locations = response.json()
    
    Notes
    -----
    File location:
        - Production: /home/stravis/backyardbuoys_files/python_scripts/info_jsons/
        - Development: C:/Users/APLUser/NANOOS/Backyard_Buoys/python_scripts/info_jsons/
    
    API Endpoints:
        - get_locations: Returns list of all buoy deployment locations
        - get_location_data: Returns time-series data for a location
        - get_platform_data: Returns data for a specific buoy/platform
    
    See Also
    --------
    backyardbuoys_dataaccess.bbapi_get_locations : Use locations endpoint
    backyardbuoys_dataaccess.bbapi_get_location_data : Use location data endpoint
    backyardbuoys_dataaccess.bbapi_get_platform_data : Use platform data endpoint
    """
    
    # Get the current working directory to determine environment
    basedir = os.path.dirname(__file__)

    # Load in the directory info json
    with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    
    # Get the base directory which contains the erddap data
    infodir = dir_info['info_jsons']
    
    # Open and load the API configuration JSON file
    with open(os.path.join(infodir, 'bbapi_info.json'), 'r') as f:
        bbapiinfo = json.load(f)
    
    return bbapiinfo


def send_emailreport(msgtxt, subj, fromaddr=None, toaddr=None,
                     login=None, passwd=None, smtpserver=None, htmlflag=False):
    
    # Function: send_emailreport
    # This function is used to send automatic emails to the data manager
    
    import smtplib
    from email.message import EmailMessage

    if (fromaddr is None) or (toaddr is None) or (login is None) or (passwd is None) or (smtpserver is None):
        print('Not enough email parameters provided - email not sent.')
        return
        
    
    # Create the email message
    msg = EmailMessage()
    msg['Subject'] = subj
    msg['From'] = fromaddr
    msg['To'] = toaddr
    if htmlflag:
        msg.set_content(msgtxt, subtype='html')
    else:
        msg.set_content(msgtxt)
    
    # Login to the email server
    server = smtplib.SMTP(smtpserver)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(login, passwd)
    
    
    # Send the message
    problems = server.send_message(msg)
    
    # Logout of the email server
    server.quit()
    
    return
