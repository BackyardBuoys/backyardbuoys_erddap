#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import xml
import xml.etree.ElementTree as ET

import os
import shutil

import datetime
import pandas as pd

import backyardbuoys_general_functions as bb
import backyardbuoys_dataaccess as bb_da
import backyardbuoys_processdata as bb_process


# In[ ]:


def get_datasetxml_dir():
    
    import os
    
    curdir = os.getcwd()
    if '/home/stravis/' in curdir:
        basedir = '/home/stravis/backyardbuoys_files/erddap_files/'
    else:
        basedir = 'C:/Users/APLUser/NANOOS/Backyard_Buoys/erddap_files/'
        
    return basedir


# In[ ]:


def update_dataset_template(snip_root, meta):
    
    #######################
    # Update the dataset ID
    snip_root.attrib['datasetID'] = 'backyardbuoys_' + meta['location_id']
    
    
    ##############################
    # Update the data folder name structure
    snip_root.find('fileDir').text = '/data/tomcat/dataset_files/' + meta['location_id'] + '/'
    
    
    ##############################
    # Update the filename structure
    snip_root.find('fileNameRegex').text = "bb_" + meta['location_id'] + r"_.*\.nc"
    
    #######################
    # Update the Attributes
    for elem in snip_root.find('addAttributes').findall('att'):
        if elem.attrib['name'] == 'institution':
            elem.attrib['name'] = meta['creator_institution']
    
    
    return snip_root


# In[ ]:


def load_main_dataset_xml():
    
    #############################################
    # Get the base directory for all dataset xmls
    dataset_dir = get_datasetxml_dir()
    
    
    #############################################
    # Load in the root dataset
    datafile = 'datasets.xml'
    if os.path.exists(os.path.join(dataset_dir, datafile)):
        
        # Backup a copy of the existing dataset.xml file
        if not(os.path.exists(os.path.join(dataset_dir, 'archive'))):
            os.mkdir(os.path.join(dataset_dir, 'archive'))
        backup_file = 'datasets_' + datetime.datetime.now().strftime('%Y%m%d') + '.xml'
        shutil.copy(os.path.join(dataset_dir, datafile),
                    os.path.join(dataset_dir, 'archive', backup_file))
        
        # Read in the master datasets.xml
        main_tree = ET.parse(os.path.join(dataset_dir, datafile))
        
    else:
        
        basefile = 'base_datasets.xml'
        
        # Read in an empty master datasets.xml
        main_tree = ET.parse(os.path.join(dataset_dir, basefile))
        
    return main_tree        


# In[ ]:


def sort_datasets_alphabetically(main_root):
    
    # Sort all the dataset IDs
    sorted_root = sorted(main_root.findall('dataset'), 
                         key=lambda x: x.attrib['datasetID'])

    # Remove all of the existing datasets from the root
    for elem in main_root.findall('dataset'):
        if 'datasetID' in elem.attrib:
            main_root.remove(elem)

    # Add on the sorted datasets
    main_root.extend(sorted_root)
    
    return main_root


# In[ ]:


def update_datasets_xml(loc_ids):
    
    #############################################
    # Get the base directory for all dataset xmls
    dataset_dir = get_datasetxml_dir()
    
    #############################################
    # Load in the root dataset
    newfile = 'datasets.xml'
    main_tree = load_main_dataset_xml()
    main_root = main_tree.getroot()
    
    
    ############################################################
    # Update the template snippet for the appropriate location
    if not(isinstance(loc_ids, list)):
        loc_ids = [loc_ids]
        
    # Step through each location ID to add, and update the main root
    # to add and/or replace that dataset
    for loc_id in loc_ids:
        main_root = add_new_dataset_snip(dataset_dir, main_root, loc_id)
        
    # Sort the datasets alphabetically
    main_root = sort_datasets_alphabetically(main_root)
        
        
    #################################
    # Write a new master datasets.xml
    main_tree.write(os.path.join(dataset_dir, newfile), 
                    encoding="ISO-8859-1", xml_declaration=True)
    
    return


# In[ ]:


def add_new_dataset_snip(dataset_dir, main_root, loc_id):
    
    #############################################################
    # Read in the individual xml template snippet for one dataset
    template = 'dataset_template.xml'
    snip_tree = ET.parse(os.path.join(dataset_dir, template))
    snip_root = snip_tree.getroot()
    
    # Update the snip with the location metadata
    meta = bb.get_location_metadata(loc_id)
    snip_root = update_dataset_template(snip_root, meta)


    ###########################################################
    # Append on the new dataset to the main dataset file

    # Get a list of all of the existing dataset names
    dataset_names = []
    for elem in main_root.findall('dataset'):
        if 'datasetID' in elem.attrib:
            dataset_names.append(elem.attrib['datasetID'])

    snip_dataset_name = snip_root.attrib['datasetID']
    append_new = True
    if any([ii == snip_dataset_name for ii in dataset_names]):
        append_new = False

    # Insert the updated snippet into master datasets.xml
    if append_new:
        main_root.append(snip_root)
    else:
        for elem in main_root.findall('dataset'):
            if 'datasetID' in elem.attrib:
                if elem.attrib['datasetID'] == snip_dataset_name:
                    print('Remove the existing element for: ' + snip_dataset_name)
                    main_root.remove(elem)
        main_root.append(snip_root)
        
    return main_root


# In[ ]:


def add_all_datasets():
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations() 
    
    full_locs = []
    for bb_loc in bb_locs:

        if bb_process.get_location_metadata(bb_locs[bb_loc]['loc_id']) is not None:
            full_locs.append(bb_locs[bb_loc]['loc_id'])
            
            
    # Use the full list of locs to update the dataset xml
    update_datasets_xml(full_locs)


# In[ ]:




