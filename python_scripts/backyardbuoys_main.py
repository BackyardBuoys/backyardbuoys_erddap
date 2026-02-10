#!/usr/bin/env python
# coding: utf-8

"""
BackyardBuoys ERDDAP - Main Entry Point
========================================

This module provides the command-line interface for managing the BackyardBuoys
ERDDAP data pipeline. It handles data processing, metadata compilation, and
ERDDAP XML configuration.

Command-line Arguments:
    -h, --help      : Display help information
    -p, --process   : Process type (addData, addDataset, addMetadata)
    -l, --location  : Location ID or "all"
    -r, --rebuild   : Rebuild datasets from scratch (true/false)
    -q, --qctests   : Rerun quality control tests (true/false)

Example Usage:
    # Update data for a single location
    python backyardbuoys_main.py -p addData -l quileute_south
    
    # Rebuild all datasets with new QC tests
    python backyardbuoys_main.py -p addData -l all -r true -q true
    
    # Add metadata for a location
    python backyardbuoys_main.py -p addMetadata -l gambell
    
    # Add dataset to ERDDAP
    python backyardbuoys_main.py -p addDataset -l quileute_south

Author: Seth Travis
Organization: Backyard Buoys
"""

import json
import os
import sys
import getopt
import gc
import warnings
import traceback

# Import BackyardBuoys modules
import backyardbuoys_general_functions as bb
import backyardbuoys_processdata as bb_process
import backyardbuoys_build_metadata as bb_meta
import backyardbuoys_generate_xml as bb_xml

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")


def main():
    """
    Main entry point for the BackyardBuoys ERDDAP processing pipeline.
    
    Parses command-line arguments and executes the specified workflow.
    Validates all inputs before processing to ensure data integrity.
    
    Processes:
        addData     : Add/update location data with optional rebuild and QC rerun
        addDataset  : Add location dataset to ERDDAP datasets.xml
        addMetadata : Create/update metadata JSON for location
    
    Returns
    -------
    None
        Exits with sys.exit(2) after completion or error
    
    Raises
    ------
    SystemExit
        On invalid arguments or completion of processing
    """
    
    # ========================================================================
    # Parse Command-Line Arguments
    # ========================================================================
    try:
        # Parse options and arguments
        # Short options: h, u, p:, l:, r:, q:
        # Long options: help, process=, location=, rebuild=, qctests=
        opts, args = getopt.getopt(
            sys.argv[1:], 
            "hu:p:l:r:q:", 
            ["help", "process=", "location=", "rebuild=", "qctests"]
        )
    except Exception as inst:
        # Print error if option parsing fails
        print('Error in getting options: ' + str(inst))
        sys.exit(2)

    # ========================================================================
    # Initialize Flags for Command-Line Options
    # ========================================================================
    # These flags track which command-line options were provided
    processFlag = False    # Was -p/--process provided?
    locFlag = False        # Was -l/--location provided?
    rebuildFlag = False    # Was -r/--rebuild provided?
    qctestFlag = False     # Was -q/--qctests provided?

    # ========================================================================
    # Process Each Command-Line Option
    # ========================================================================
    # Loop through all provided options and extract their values
    for o, a in opts:
        if o == "-v":
            # Verbose mode (not currently implemented)
            bVerbose = True
            
        elif o in ("-h", "--help"):
            # Display comprehensive help information
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
            print('     "addWMO":      Add/update the WMO code for a given location to all data files')
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
            # Set process flag and store process name
            processFlag = True
            processName = a
            
        elif o in ("-l", "--location"):
            # Set location flag and store location name
            locFlag = True
            locName = a
            
        elif o in ("-r", "--rebuild"):
            # Set rebuild flag and store rebuild option
            rebuildFlag = True
            rebuildName = a
            
        elif o in ("-q", "--qctests"):
            # Set QC test flag and store qctest option
            qctestFlag = True
            qctestName = a
        else:
            # Catch any unhandled options
            assert False, "unhandled option"

    # ========================================================================
    # Validate Process Option
    # ========================================================================
    if processFlag:
        # Ensure the process name is a string
        if not(isinstance(processName, str)):
            print('Invalid process given. Unable to proceed.')
            print('Valid processes are:')
            print('"addData":     Add new data/update all data for a given location')
            print('               Note that this process can use the "rebuild" flag')
            print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
            print('"addMetadata": Add/update the metadata json for a given location')
            print('"addWMO":      Add/update the WMO code for a given location to all data files')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2)
        
        # Check that the process name is one of the valid types
        valid_procs = ['addData', 'addDataset', 'addMetadata', 'addWMO']
        if not(any([ii == processName for ii in valid_procs])):
            print('Invalid process given. Unable to proceed.')
            print('Valid processes are:')
            print('"addData":     Add new data/update all data for a given location')
            print('               Note that this process can use the "rebuild" flag')
            print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
            print('"addMetadata": Add/update the metadata json for a given location')
            print('"addWMO":      Add/update the WMO code for a given location to all data files')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            sys.exit(2)
    else:
        # No process was provided - print error and exit
        print('No process given. Unable to proceed.')
        print('Valid processes are:')
        print('"addData":     Add new data/update all data for a given location')
        print('               Note that this process can use the "rebuild" flag')
        print('"addDataset":  Add a new dataset to the ERDDAP dataset xml file')
        print('"addMetadata": Add/update the metadata json for a given location')
        print('"addWMO":      Add/update the WMO code for a given location to all data files')
        print('\nPlease restart the program again, using a minimum syntax of:')
        print("backyardbuoys_main -p <processName> -l <locationName>")
        print('If you need addition options, please try:')
        print("backyardbuoys_main -h")
        sys.exit(2)
        
    # ========================================================================
    # Validate Rebuild Flag Option
    # ========================================================================
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
            
        # Convert the rebuild option string to a boolean value
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
            
        # Check if rebuild flag is being used with an incompatible process
        # The rebuild flag can only be used with the 'addData' process
        if (not((processName == 'addData') or (processName == 'addMetadata'))) and rebuildFlag:
            print('The rebuild flag can only be used for the "addData" or "addMetadata" processes.')
            print('Flag will be set to "false", and process will continue.')
            rebuildFlag = False
        
    # ========================================================================
    # Validate QC Tests Flag Option
    # ========================================================================
    if qctestFlag:
        # Ensure that the qctest name is a string
        if not(isinstance(qctestName, str)):
            print('Invalid option given for qctests flag.')
            print('Valid options include "true"/"false"')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            # Exit the program unsuccessfully
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
            # Exit the program unsuccessfully
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
            # Exit the program unsuccessfully
            sys.exit(2) 
            
        
        # If the qctestFlag is set to true, but the process is not
        # "allData", then the qctestFlag will be set to false
        if not(processName == 'addData') and qctestFlag:
            print('The qctests flag can only be used for the "addData" process.')
            print('Flag will be set to "false", and process will continue.')
            qctestFlag = False
        
        
    # ================================================ #
    # Determine that the location is valid, and check  #
    # if the location is "all"                         #
    # ================================================ #
    if locFlag:
        # Ensure that the location name is a string
        if not(isinstance(locName, str)):
            print('No location given. Unable to proceed.')
            print('Note: to run the process for all sites, use a location of "all".')
            print('\nPlease restart the program again, using a minimum syntax of:')
            print("backyardbuoys_main -p <processName> -l <locationName>")
            print('If you need addition options, please try:')
            print("backyardbuoys_main -h")
            # Exit the program unsuccessfully
            sys.exit(2)

        # Check if user wants to process all locations with rebuild flag
        # This is a potentially destructive operation, so confirm with user    
        allFlag = False
        if (locName.lower() == 'all') and rebuildFlag:
            print('Process will be performed on all datasets.')
            checkInput = input('Are you sure that you want to do this? (y/n) ')
            if checkInput.lower() == 'y':
                allFlag = True
            elif checkInput.lower() == 'n':
                print('Process will not be performed on all datasets.')
                print('Please restart the program with the correct location name.')
                # Exit the program unsuccessfully
                sys.exit(2)
            else:
                print('Invalid selection: ' + checkInput)
                print('Process will not be performed on all datasets.')
                print('Please restart the program with the correct location name.')
                # Exit the program unsuccessfully
                sys.exit(2)
    else:
        # No location was provided - print error and exit
        print('No location given. Unable to proceed.')
        print('Note: to run the process for all sites, use a location of "all".')
        print('\nPlease restart the program again, using a minimum syntax of:')
        print("backyardbuoys_main -p <processName> -l <locationName>")
        print('If you need addition options, please try:')
        print("backyardbuoys_main -h")
        # Exit the program unsuccessfully
        sys.exit(2)
            
    
    # ============================================= #
    # Set up error email function              #    #
    # ============================================= #   

    # Create a flag to determine whether to send error emails
    send_error_email_flag = True

    # Create a function to send error emails
    def send_error_email(processName, locName, e):

        # Load in the directory info json
        basedir = os.path.dirname(__file__)
        with open(os.path.join(basedir,'bb_dirs.json'), 'r') as dir_json:
            dir_info = json.load(dir_json)
        # Get the base directory which contains the erddap data
        infodir = dir_info['info_jsons']
        with open(os.path.join(infodir, 'user_info.json'), 'r') as infofile:
            user_info = json.load(infofile)

        error_msg = "An error occured while performing " + processName + " for location " + locName + ".\n\n" + "Error message:\n" + str(e)
        error_sbj = "Backyard Buoys - Error in " + processName + "; Location: " + locName
        
        bb.send_emailreport(msgtxt=error_msg, subj=error_sbj,
                            fromaddr=user_info['email_fromaddr'], toaddr="setht1@uw.edu",
                            login=user_info['email_login'], passwd=user_info['email_passwd'],
                            smtpserver=user_info['smtpserver'])


    # ============================
    # Run the specified process
    # ============================
    try:
        if processName == 'addData':
            # Process: Add or update data for location(s)
            # Calls data processing functions with rebuild and qctest flags
            if locName.lower() == 'all':
                # Update data for all active locations
                bb_process.update_all_locations(
                    rebuild_flag=rebuildFlag, 
                    rerun_tests=qctestFlag
                )
            else:
                # Update data for a single location
                bb_process.update_data_by_location(
                    locName, 
                    rebuild_flag=rebuildFlag, 
                    rerun_tests=qctestFlag
                )
        
        elif processName == 'addDataset':
            # Process: Add dataset entry to ERDDAP datasets.xml
            # Generates XML configuration for ERDDAP server
            if locName.lower() == 'all':
                # Add all locations with valid metadata to ERDDAP
                bb_xml.add_all_datasets()
            else:
                # Add specific location to ERDDAP
                bb_xml.update_datasets_xml(locName)
            
        elif processName == 'addMetadata':
            # Process: Create or update metadata JSON files
            # Pulls metadata from Google Sheets and generates JSON files
            if locName.lower() == 'all':
                # Create/update metadata for all locations
                bb_meta.make_projects_metadata(rebuild_flag=rebuildFlag)
            else:
                # Create/update metadata for specific location
                bb_meta.make_projects_metadata(locName, rebuild_flag=rebuildFlag)
            
        elif processName == 'addWMO':
            if locName.lower() == 'all':
                print('The "addWMO" process can only be run for a single location at a time.')
                print('Please restart the program with a specific location name.')
                # Exit the program unsuccessfully
                sys.exit(2)

            # First, recreate/update metadata for specific location
            bb_meta.make_projects_metadata(locName, rebuild_flag=rebuildFlag)

            # Process: Add or update WMO code for a given location
            successFlag = bb_process.add_wmo_code_to_data(locName)
            if not successFlag:
                print('Failed to add/update WMO code for location: ' + locName)
                # Exit the program unsuccessfully
                sys.exit(2)
            
            
        else:
            # Invalid process name provided (should not reach here due to earlier validation)
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
            # Exit the program unsuccessfully
            sys.exit(2)
    except Exception as e:
        print('An error occurred while performing the process: ' + processName)
        print(e)
        traceback.print_exc()
        if send_error_email_flag:
            send_error_email(processName, locName, e)
        # Exit the program unsuccessfully
        sys.exit(2)


    # Exit the program successfully
    sys.exit(0)


# ============================================================================
# Main Execution Block
# ============================================================================
if __name__ == "__main__":
    # Execute main function if script is run directly
    main()
