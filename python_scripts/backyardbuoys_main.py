#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import sys
import getopt
import gc

import backyardbuoys_general_functions as bb
import backyardbuoys_processdata as bb_process
import backyardbuoys_build_metadata as bb_meta
import backyardbuoys_generate_xml as bb_xml

import warnings
warnings.filterwarnings("ignore")


# In[ ]:


def main():

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:p:l:r:q:", 
                                   ["help", "process=", "location=", "rebuild=", "qctests"])
    except Exception as inst:
        # print help information and exit:
        print('Error in getting options: ' + str(inst)) # will print something like "option -a not recognized"
        sys.exit(2)


    # step through command-line options and their arguments
    processFlag = False
    locFlag = False
    rebuildFlag = False
    qctestFlag = False

    # For all the command-line arguments, update a flag to indicate if the option was given,
    # and extract the value of the command-line argument
    for o, a in opts:
        if o == "-v":
            bVerbose = True
            # but currently verbose run is not implemented!
        elif o in ("-h", "--help"):
            print('This program is used to update the Backyard Buoys data repositories, ')
            print('and to aid in posting them to the Backyard Buoys ERDDAP server.')
            print('\nTo use this program, the following options can be passed:')
            print('   -h/help')
            print('   -p/process')
            print('   -l/location')
            print('   -r/rebuild')
            print('   -q/qctests')
            print('\n  "help":')
            print('     Help listing to provide information on using the project')
            print('\n  "process":')
            print('     Valid processes are:')
            print('     "addData":     Add new data/update all data for a given location')
            print('                    Note that this process can use the "rebuild" flag')
            print('     "addDataset":  Add a new dataset to the ERDDAP dataset xml file')
            print('     "addMetadata": Add/update the metadata json for a given location')
            print('\n  "location":')
            print('     Locations are the matching location_ids taken from the')
            print('     Backyard Buoys API Server.')
            print('     (https://data.backyardbuoys.org/get_locations)')
            print('      Additional option is to include "all" locations')
            print('     i.e. include flag as , "-l all"')
            print('\n  "rebuild":')
            print('     Flags whether the datasets should be rebuilt.')
            print('     Note: this can only be used from the "addData" process')
            print('     Valid rebuild flags are "true"/"false"')
            print('\n  "qctests":')
            print('     Flags whether quality control tests for the datasets should be rerun.')
            print('     Note: this can only be used from the "addData" process')
            print('     Valid qctests flags are "true"/"false"')
            sys.exit()
            
        elif o in ("-p", "--process"):
            processFlag = True
            processName = a
        elif o in ("-l", "--location"):
            locFlag = True
            locName = a
        elif o in ("-r", "--rebuild"):
            rebuildFlag = True
            rebuildName = a
        elif o in ("-q", "--qctests"):
            qctestFlag = True
            qctestName = a
        else:
            assert False, "unhandled option"


    ##############################################
    # Determine that a process has been given, and 
    # that it is a valid process
    if processFlag:
        # Ensure that the process name is a string
        if not(isinstance(processName, str)):
            print('Invalid process given. Unable to proceed.')
            print('Valid processes are:')
            print('"addData":     Add new data/update all data for a given location')
            print('               Note that this process can use the "rebuild" flag')
            print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
            print('"addMetadata": Add/update the metadata json for a given location')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2)
        
        
        # Check that the process name matches one of the valid types
        valid_procs = ['addData', 'addDataset', 'addMetadata']
        if not(any([ii == processName for ii in valid_procs])):
            print('Invalid process given. Unable to proceed.')
            print('Valid processes are:')
            print('"addData":     Add new data/update all data for a given location')
            print('               Note that this process can use the "rebuild" flag')
            print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
            print('"addMetadata": Add/update the metadata json for a given location')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2)
    else:
        print('No process given. Unable to proceed.')
        print('Valid processes are:')
        print('"addData":     Add new data/update all data for a given location')
        print('               Note that this process can use the "rebuild" flag')
        print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
        print('"addMetadata": Add/update the metadata json for a given location')
        print('\nPlease restart the program again, using a minimum syntax of:')
        print("backyardbuoys_main -p <processName> -l <locationName>")
        print('If you need addition options, please try:')
        print("backyardbuoys_main -h")
        sys.exit(2)
        
        
    #####################################################
    # Determine if the rebuild flag was given
    if rebuildFlag:
        # Ensure that the rebuild name is a string
        if not(isinstance(rebuildName, str)):
            print('Invalid option given for rebuild flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2) 
    
        # Ensure that the rebuild name is one of the valid options
        if not((rebuildName.lower() == 'true') 
               or (rebuildName.lower() == 'false')):
            print('Invalid option given for rebuild flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2) 
            
        # Create a rebuild option to pass to functions which matches
        # the option given
        if rebuildName.lower() == 'true':
            rebuildFlag = True
        elif rebuildName.lower() == 'false':
            rebuildFlag = False
        else:
            print('Invalid option given for rebuild flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2) 
            
        
        # If the rebuildFlag is set to true, but the process is not
        # "allData", then the rebuildFlag will be set to false
        if not(processName == 'addData') and rebuildFlag:
            print('The rebuild flag can only be used for the "addData" process.')
            print('Flag will be set to "false", and process will continue.')
            rebuildFlag = False
        
        
    #####################################################
    # Determine if the qctests flag was given
    if qctestFlag:
        # Ensure that the rebuild name is a string
        if not(isinstance(qctestName, str)):
            print('Invalid option given for qctests flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2) 
    
        # Ensure that the rebuild name is one of the valid options
        if not((qctestName.lower() == 'true') 
               or (qctestName.lower() == 'false')):
            print('Invalid option given for qctests flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2) 
            
        # Create a rebuild option to pass to functions which matches
        # the option given
        if qctestName.lower() == 'true':
            qctestFlag = True
        elif qctestName.lower() == 'false':
            qctestFlag = False
        else:
            print('Invalid option given for qctests flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2) 
            
        
        # If the qctestFlag is set to true, but the process is not
        # "allData", then the qctestFlag will be set to false
        if not(processName == 'addData') and qctestFlag:
            print('The qctests flag can only be used for the "addData" process.')
            print('Flag will be set to "false", and process will continue.')
            qctestFlag = False
        
        
    #################################################
    # Determine that the location is valid, and check
    # if the location is "all"
    if locFlag:
        # Ensure that the location name is a string
        if not(isinstance(locName, str)):
            print('No location given. Unable to proceed.')
            print('Note: to run the process for all sites, use a location of "all".')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2)
            
        allFlag = False
        if (locName.lower() == 'all') and rebuildFlag:
            print('Process will be performed on all datasets.')
            checkInput = input('Are you sure that you want to do this? (y/n) ')
            if checkInput.lower() == 'y':
                allFlag = True
            elif checkInput.lower() == 'n':
                print('Process will not be performed on all datasets.')
                print('Please restart the program with the correct location name.')
                sys.exit(2)
            else:
                print('Invalid selection: ' + checkInput)
                print('Process will not be performed on all datasets.')
                print('Please restart the program with the correct location name.')
                sys.exit(2)
    else:
        print('No location given. Unable to proceed.')
        print('Note: to run the process for all sites, use a location of "all".')
        print('\nPlease restart the program again, using a minimum syntax of:')
        print("backyardbuoys_main -p <processName> -l <locationName>")
        print('If you need addition options, please try:')
        print("backyardbuoys_main -h")
        sys.exit(2)
            
            


    ############################
    # Run the specified process
    if processName == 'addData':
        if locName.lower() == 'all':
            bb_process.update_all_locations(rebuild_flag=rebuildFlag, rerun_tests=qctestFlag)
        else:
            bb_process.update_data_by_location(locName, rebuild_flag=rebuildFlag, rerun_tests=qctestFlag)
    
    elif processName == 'addDataset':
        if locName.lower() == 'all':
            bb_xml.add_all_datasets()
        else:
            bb_xml.update_datasets_xml(locName)
        
    elif processName == 'addMetadata':
        if locName.lower() == 'all':
            bb_meta.make_all_metadata()
        else:
            bb_meta.make_project_metadata(locName)
        
        
    else:
        print('Invalid process given. Unable to proceed.')
        print('Valid processes are:')
        print('"addData":     Add new data/update all data for a given location')
        print('               Note that this process can use the "rebuild" flag')
        print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
        print('"addMetadata": Add/update the metadata json for a given location')
        print('Please restart the program again, using a minimum syntax of:')
        print("backyardbuoys_main -p <processName> -l <locationName>")
        print('If you need addition options, please try:')
        print("backyardbuoys_main -h")
        sys.exit(2)


    # Exit the program
    sys.exit(2)


# In[ ]:


if __name__ == "__main__":
    main()

