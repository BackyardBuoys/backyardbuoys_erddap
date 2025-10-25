#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
This collection of functions are general
functions that would be used across a range
of Backyard Buoys data processing scripts.

These functions include loading in json files,
defining data directories, and others.
"""


# In[ ]:


def get_location_metadata(loc_id):
    
    """
    This function identifies the directory
    that contains the metadata jsons for a given
    location ID, and loads in that json file.
    
    Function inputs:
    loc_id - the location ID, taken from the Backyard
             Buoys API, for a given dataset
             
    Function outputs:
    metadata - the metadata for a given location ID,
               loaded in from the dataset metadata json
    """
    
    import os
    import json
    
    # Load in the meta data for all locations
    basedir = get_datadir()
    pathdir = os.path.join(basedir, loc_id, 'metadata', loc_id+'_metadata.json')
    
    if not(os.path.exists(pathdir)):
        return None
    else:
        
        with open(pathdir) as meta_json:
            meta = json.load(meta_json)
    
        
    return meta['metadata']


# In[ ]:


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
    
    # Load in the meta data for all locations
    basedir = get_datadir()
    pathdir = os.path.join(basedir, loc_id, 'metadata', loc_id+'_info.json')
    
    if not(os.path.exists(pathdir)):
        return None
    else:
        
        with open(pathdir) as info_json:
            loc_info = json.load(info_json)
    
        
    return loc_info


# In[ ]:


def get_datadir():
    
    """
    This function identifies the base directory
    that contains the ERDDAP data files
    
    Function inputs:
    None
    
    Function outputs:
    basedir - the directory path to the folder
              containing the data files accessed
              by the ERDDAP service
    """
    
    import os
    import json
    
    # Get the current working directory
    curdir = os.getcwd()
    # Load in the directory info json
    with open(os.path.join(curdir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the erddap data
    basedir = dir_info['erddap_data']
        
    return basedir


# In[ ]:


def load_googleinfo_json():
    
    """
    This function identifies the directory that
    contains the data location jsons, and reads
    in the info for Google connections
    
    Function inputs:
    None
    
    Function outputs:
    googleinfo - the dictionary containing
                 the info needed to access the
                 Google Drive API
    """
    
    import os
    import json
    
    # Get the current working directory
    curdir = os.getcwd()
    # Load in the directory info json
    with open(os.path.join(curdir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the erddap data
    basedir = dir_info['info_jsons']
    
    # Using the base directory that contains the info jsons,
    # load in the json for the Google paths
    with open(os.path.join(basedir, 'google_info.json'), 'r') as f:
        googleinfo = json.load(f)
    
    return googleinfo


# In[ ]:


def load_bbapi_info_json():
    
    """
    This function identifies the directory that
    contains the data location jsons, and reads
    in the info for Backyard Buoys API connections
    
    Function inputs:
    None
    
    Function outputs:
    bbapiinfo - the dictionary containing
                the info needed to access the
                Backyard Buoys API
    """
    
    import os
    import json
    
    # Get the current working directory
    curdir = os.getcwd()
    # Load in the directory info json
    with open(os.path.join(curdir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the erddap data
    basedir = dir_info['info_jsons']
    
    # Use the base directory to load in the Backyard Buoys
    # API info json
    with open(os.path.join(basedir, 'bbapi_info.json'), 'r') as f:
        bbapiinfo = json.load(f)
    
    return bbapiinfo

