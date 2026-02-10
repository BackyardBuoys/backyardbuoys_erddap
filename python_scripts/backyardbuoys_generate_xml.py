#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import xml
import xml.etree.ElementTree as ET

import os
import shutil

import datetime
import pandas as pd
import numpy as np

import backyardbuoys_general_functions as bb
import backyardbuoys_dataaccess as bb_da
import backyardbuoys_processdata as bb_process

import json


# In[ ]:


"""
This collection of functions is dedicated towards generating
the "datasets.xml" file which is used by the Backyard Buoys ERDDAP
server in building out the datasets to post online.
"""


# In[ ]:

# Create a function to send error emails
def send_newdataset_email(locName, smart_flag=False):

    # Load in the directory info json
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory which contains the erddap data
    infodir = dir_info['info_jsons']
    with open(os.path.join(infodir, 'user_info.json'), 'r') as infofile:
        user_info = json.load(infofile)

    newdata_msg = "A new dataset has been added for location " + locName + ".\n\n"
    if smart_flag:
        newdata_msg = newdata_msg + "This location contains Smart Mooring data.\n\n"
        newdata_sbj = "Backyard Buoys - New dataset (with smart mooring) added: Location: " + locName
    else:
        newdata_sbj = "Backyard Buoys - New dataset added: Location: " + locName
    
    bb.send_emailreport(msgtxt=newdata_msg, subj=newdata_sbj,
                        fromaddr=user_info['email_fromaddr'], toaddr="setht1@uw.edu",
                        login=user_info['email_login'], passwd=user_info['email_passwd'],
                        smtpserver=user_info['smtpserver'])


def get_datasetxml_dir():
    
    """
    This function identifies the base directory
    that contains the ERDDAP related info files
    (i.e., the datasets.xml file)
    
    Function inputs: 
    None
    
    Function outputs: 
    basedir - base directory path containing ERDDAP info files
    """
    
    # Get the current working directory
    basedir = os.path.dirname(__file__)

    # Load in the directory info json
    with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
        dir_info = json.load(dir_json)
    # Get the base directory for the ERDDAP files
    xmldir = dir_info['erddap_files']
        
    return xmldir


# In[ ]:


def update_dataset_template(snip_root, meta, smartflag=False):
    
    """
    This function takes a snip of the dataset template,
    and use the metadata to update the relevant fields in
    the xml snippet.
    
    Function inputs:
    snip_root - xml snippet of the dataset template
    meta      - dictionary containing the metadata for the location
    
    Function outputs:
    snip_root - xml snippet of the dataset template
                that has been updated with the metadata
    """
    
    datasetID = 'backyardbuoys_' + meta['location_id']
    fileDir = '/data/tomcat/dataset_files/' + meta['location_id']
    fileNameRegex = "bb_" + meta['location_id']
    
    if smartflag:
        datasetID = datasetID + '_smart'
        fileNameRegex = fileNameRegex + '_smart' + r"_.*\.nc"
    else:
        fileNameRegex =  fileNameRegex + '(?!_smart)' + r"_.*\.nc"
    
    #######################
    # Update the dataset ID
    snip_root.attrib['datasetID'] = datasetID
    
    
    ##############################
    # Update the data folder name structure
    snip_root.find('fileDir').text = fileDir
    
    
    ##############################
    # Update the filename structure
    snip_root.find('fileNameRegex').text = fileNameRegex
    
    #######################
    # Update the Attributes
    for elem in snip_root.find('addAttributes').findall('att'):
        if elem.attrib['name'] == 'institution':
            elem.attrib['name'] = meta['creator_institution']
    
    
    return snip_root


# In[ ]:


def load_main_dataset_xml():
    
    """
    This function loads in the main
    "datatsets.xml" file.
    
    If there already exists a "datasets.xml"
    file, that file is loaded in. If not,
    the empty "base_datasets.xml" file
    is loaded.
    
    Function inputs:
    None
    
    Function outputs:
    main_tree - the main tree of the loaded
                datasets xml file
    """
    
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
    
    """
    This function uses the main_root of the
    datasets xml tree, reads through all of
    the dataset IDs, sorts them into
    alphabetical order, and then rewrites the
    xml file.
    
    Function inputs:
    main_root - the main root of the datasets 
                xml file
                
    Function outputs:
    main_root - the main root of the datasets
                xml file which have been sorted
    """
    
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
    
    """
    This function loads in the datasets xml
    file, and updates it to add in new snippets
    for each location id
    
    Function inputs:
    loc_ids: list of location IDs, as given
             by the Backyard Buoys API
             
    Function outputs:
    None
    """
    
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


def add_new_smart_dataset_snip(dataset_dir, main_root, loc_id):

    """
    The function adds a new smart dataset snippet to
    the main datasets.xml file
    
    Function inputs:
    dataset_dir - the directory path to the location
                  where the ERDDAP dataset xml files are located
    main_root   - the main root of the datasets.xml file
    loc_id      - the location ID, taken from the Backyard Buoys
                  API, for the dataset to be added
                  
    Function outputs:
    main_root   - the main root of the datasets.xml file,
                  which has been updated to add the snippet
                  for the location ID
    """
    
    print('Attempt to add dataset snip for location ID: ' + loc_id + '_smart')

    ################################################
    # Load in the location info
    info = bb.get_location_info(loc_id)
    
    # Check for smart mooring buoys
    if info['spotter_ids'] == '':
        spotter_list = []
    else:
        spotter_list = [ii.strip() for ii in np.unique(info['spotter_ids'].split(','))]
    
    # Get a list of all of the variables in the smart mooring
    smartvars = []
    for spotter_id in spotter_list:
        for smartvar in info['spotter_data'][spotter_id]['smart_mooring_info']:
            smartvars.append(smartvar['var_id'])
    smartvars = [ii for ii in np.unique(smartvars)]
    
    
    #############################################################
    # Read in the individual xml template snippet for one dataset
    template = 'dataset_smart_template.xml'
    snip_tree = ET.parse(os.path.join(dataset_dir, template))
    snip_root = snip_tree.getroot()

    # Read in the location metadata
    meta = bb.get_location_metadata(loc_id)

    # Update the snip with the location metadata
    snip_root = update_dataset_template(snip_root, meta, smartflag=True)
    
    
    ###########################################################
    # Update the snip root with all of the info for all the
    # variables for the smart mooring
    for smartvar in smartvars:

        # Get smart mooring variable info
        smartvar_info = smartmooring_vars_list(smartvar)

        # Add the specific smart mooring variable info
        smartvar_xmlfile = os.path.join(dataset_dir, 'smartvars', 
                                        smartvars[0]+'.xml')
        smartvar_tree = ET.parse(smartvar_xmlfile)
        smartvar_snip = smartvar_tree.getroot()
        
        snip_root.append(smartvar_snip)

        # Add generic QC flag info for the smart mooring variable
        smartvar_xmlfile = os.path.join(dataset_dir, 'smartvars', 'QCFlags.xml')
        smartvar_tree = ET.parse(smartvar_xmlfile)
        smartvar_snip = smartvar_tree.getroot()
        
        # Loop through each QC flag variable and update the names
        for elem in smartvar_snip.findall('dataVariable'):

            # Update sourceName and destinationName
            sourceVar = elem.find('sourceName').text
            sourceVar = sourceVar.replace('templatevar_', smartvar_info['var_id'] + '_')
            elem.find('sourceName').text = sourceVar

            destVar = elem.find('destinationName').text
            destVar = destVar.replace('templatevar_', smartvar_info['var_id'] + '_')
            elem.find('destinationName').text = destVar

            # Update long_name attribute
            addAtts = elem.find('addAttributes')
            for att in addAtts.findall('att'):
                if att.attrib['name'] == 'long_name':
                    longnameVar = att.text
                    longnameVar = longnameVar.replace('TemplateVar', smartvar_info['long_name'])
                    att.text = longnameVar
    
            snip_root.append(elem)


    ###########################################################
    # Append on the new dataset to the main dataset file

    # Build a list of all existing dataset IDs in the XML
    dataset_names = []
    for elem in main_root.findall('dataset'):
        if 'datasetID' in elem.attrib:
            dataset_names.append(elem.attrib['datasetID'])

    # Check if this dataset already exists in the XML
    snip_dataset_name = snip_root.attrib['datasetID']
    append_new = True  # Assume it's new unless we find it
    if any([ii == snip_dataset_name for ii in dataset_names]):
        append_new = False  # Dataset already exists - will need to replace it

    # Add or update the dataset snippet in the XML
    if append_new:
        # Dataset doesn't exist yet - just append it
        main_root.append(snip_root)
    else:
        # Dataset exists - remove the old version first, then add the new one
        # This ensures we always have the most up-to-date dataset configuration
        for elem in main_root.findall('dataset'):
            if 'datasetID' in elem.attrib:
                if elem.attrib['datasetID'] == snip_dataset_name:
                    print('Remove the existing element for: ' + snip_dataset_name)
                    main_root.remove(elem)  # Remove old version
        # Append the updated snippet
        main_root.append(snip_root)
    
    # Return the updated main_root
    return main_root

def smartmooring_vars_list(varid):

    smartvars = {"WaterTemp": {"var_id": "sea_water_temperature",
                               "long_name": "Sea Water Temperature"}
                               }
    
    return smartvars[varid]


# In[ ]:


def add_new_dataset_snip(dataset_dir, main_root, loc_id):
    
    """
    The function adds a new dataset snippet to
    the main datasets.xml file
    
    Function inputs:
    dataset_dir - the directory path to the location
                  where the ERDDAP dataset xml files are located
    main_root   - the main root of the datasets.xml file
    loc_id      - the location ID, taken from the Backyard Buoys
                  API, for the dataset to be added
                  
    Function outputs:
    main_root   - the main root of the datasets.xml file,
                  which has been updated to add the snippet
                  for the location ID
    """
    
    print('Attempt to add dataset snip for location ID: ' + loc_id)
    
    ################################################
    # Load in the location info
    info = bb.get_location_info(loc_id)
    
    # Check for smart mooring buoys
    if info['spotter_ids'] == '':
        spotter_ids = []
    else:
        spotter_ids = [
            ii.strip()
            for ii in np.unique(info['spotter_ids'].split(','))
            if ii.strip()
        ]
    smartmooring = False
    spotter_data = info.get('spotter_data', {})
    for spotter_id in spotter_ids:
        if spotter_id not in spotter_data:
            print('   Warning: spotter_id not found in spotter_data: ' + spotter_id)
            continue
        if len(spotter_data[spotter_id].get('smart_mooring_info', [])):
            smartmooring = True

    if smartmooring:
        print('   Smart Mooring data present')
    
    
    #############################################################
    # Read in the individual xml template snippet for one dataset
    template = 'dataset_template.xml'
    snip_tree = ET.parse(os.path.join(dataset_dir, template))
    snip_root = snip_tree.getroot()
    
    # Read in the location metadata
    meta = bb.get_location_metadata(loc_id)
    
    # Update the snip with the location metadata
    snip_root = update_dataset_template(snip_root, meta)


    ###########################################################
    # Append on the new dataset to the main dataset file

    # Build a list of all existing dataset IDs in the XML
    dataset_names = []
    for elem in main_root.findall('dataset'):
        if 'datasetID' in elem.attrib:
            dataset_names.append(elem.attrib['datasetID'])

    # Check if this dataset already exists in the XML
    snip_dataset_name = snip_root.attrib['datasetID']
    append_new = True  # Assume it's new unless we find it
    if any([ii == snip_dataset_name for ii in dataset_names]):
        append_new = False  # Dataset already exists - will need to replace it

    # Add or update the dataset snippet in the XML
    if append_new:
        # Dataset doesn't exist yet - just append it
        main_root.append(snip_root)
        send_newdataset_email(loc_id, smart_flag=smartmooring)  
    else:
        # Dataset exists - remove the old version first, then add the new one
        # This ensures we always have the most up-to-date dataset configuration
        for elem in main_root.findall('dataset'):
            if 'datasetID' in elem.attrib:
                if elem.attrib['datasetID'] == snip_dataset_name:
                    print('Remove the existing element for: ' + snip_dataset_name)
                    main_root.remove(elem)  # Remove old version
        # Append the updated snippet
        main_root.append(snip_root)
    
    ########################
    # Smart Mooring Option #
    ########################
    # If this location has smart mooring sensors, add a separate dataset for them
    if smartmooring:
        add_new_smart_dataset_snip(dataset_dir, main_root, loc_id)
    
    
    # Return the updated main_root
    return main_root


# In[ ]:


def add_all_datasets():
    """
    This function checks all location IDs
    in the Backyard Buoys API, and adds them
    to the "datasets.xml" file.
    
    This is the function that is directly
    called from the "backyardbuoys_main.py" process.
    
    Function inputs:
    None
    
    Function outputs:
    None
    """
    
    ##################################################
    # Load in the metadata from the Google sheets,
    # and all the buoys in the Backyard Buoys data API
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations() 
    
    # Ensure that we are only making xml snippets for
    # official Backyard Buoys sites (and not for
    # "Friends of ...")
    locs_to_del = []
    for loc in bb_locs:
        if not(bb_locs[loc]['is_byb'] == 'yes'):
            locs_to_del.append(loc)
    for loc in locs_to_del:
        del bb_locs[loc]
    
    # Build a list of locations for only those
    # sites that already contain metadata jsons
    full_locs = []
    for bb_loc in bb_locs:

        if bb_process.get_location_metadata(bb_locs[bb_loc]['loc_id']) is not None:
            full_locs.append(bb_locs[bb_loc]['loc_id'])
            
            
    # Use the full list of locs to update the dataset xml
    update_datasets_xml(full_locs)
    
    return


# In[ ]:


def remove_dataset_snip(loc_id):
    
    """
    This function is used to remove any specific
    dataset from the "datasets.xml" file, based upon
    the location ID.
    
    Note that this is a stand-alone function, and
    is not explicitly called by any other script in
    the regular Backyard Buoys data processing.
    
    Function inputs:
    loc_id - the location ID for the dataset to be removed
    
    Function outputs:
    None
    """
    
    #############################################
    # Get the base directory for all dataset xmls
    dataset_dir = get_datasetxml_dir()
    
    
    #############################################
    # Load in the root dataset
    datafile = 'datasets.xml'
    if os.path.exists(os.path.join(dataset_dir, datafile)):
        # If the root datasets xml file exists, first make
        # a backup of that file, and then
        # load in the data from that xml
        
        # Backup a copy of the existing dataset.xml file
        if not(os.path.exists(os.path.join(dataset_dir, 'archive'))):
            os.mkdir(os.path.join(dataset_dir, 'archive'))
        backup_file = 'datasets_' + datetime.datetime.now().strftime('%Y%m%d') + '.xml'
        shutil.copy(os.path.join(dataset_dir, datafile),
                    os.path.join(dataset_dir, 'archive', backup_file))
        
        # Read in the master datasets.xml
        main_tree = ET.parse(os.path.join(dataset_dir, datafile))
        main_root = main_tree.getroot()
        
    else:
        # If a "datasets.xml" file does not already exist,
        # then there is no need to remove a dataset from it.
        print('No datasets file exists. Do not attempt to remove snip.')
        return
    
    # Loop through all the datasets, looking for the 
    # location ID to remove
    print('Attempt to remove dataset snip for location ID: ' + loc_id)
    for elem in main_root.findall('dataset'):
        if 'datasetID' in elem.attrib:
            if elem.attrib['datasetID'] == 'backyardbuoys_' + loc_id:
                print('Remove the existing element for: ' + 'backyardbuoys_' + loc_id)
                main_root.remove(elem)
             
            
    #################################
    # Write a new master datasets.xml
    main_tree.write(os.path.join(dataset_dir, newfile), 
                    encoding="ISO-8859-1", xml_declaration=True)
                
    return

