#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def get_location_metadata(loc_id):
    
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


def get_datadir():
    
    import os
    
    curdir = os.getcwd()
    if '/home/stravis/' in curdir:
        basedir = '/data/tomcat/dataset_files/'
    else:
        basedir = 'C:/Users/APLUser/NANOOS/Backyard_Buoys/data/'
        
    return basedir


# In[ ]:


def load_googleinfo_json():
    
    import json
    import os
    
    curdir = os.getcwd()
    if '/home/stravis/' in curdir:
        basedir = '/home/stravis/backyardbuoys_files/python_scripts/info_jsons'
    else:
        basedir = 'C:/Users/APLUser/NANOOS/Backyard_Buoys/python_scripts/info_jsons'
    
    with open(os.path.join(basedir, 'google_info.json'), 'r') as f:
        googleinfo = json.load(f)
    
    return googleinfo


# In[ ]:


def load_bbapi_info_json():
    
    import json
    import os
    
    curdir = os.getcwd()
    if '/home/stravis/' in curdir:
        basedir = '/home/stravis/backyardbuoys_files/python_scripts/bbapi_jsons'
    else:
        basedir = 'C:/Users/APLUser/NANOOS/Backyard_Buoys/python_scripts/bbapi_jsons'
    
    with open(os.path.join(basedir, 'bbapi_info.json'), 'r') as f:
        bbapiinfo = json.load(f)
    
    return bbapiinfo

