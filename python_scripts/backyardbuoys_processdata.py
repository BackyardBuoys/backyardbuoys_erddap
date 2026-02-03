#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import shutil

import numpy as np
import pandas as pd
import requests
import xarray as xr

import netCDF4
from netCDF4 import Dataset

import os
import sys
import getopt
import gc

import json


import ioos_qc
from ioos_qc import qartod
from ioos_qc.config import Config
from ioos_qc.streams import PandasStream
from ioos_qc.stores import PandasStore

import backyardbuoys_general_functions as bb    
import backyardbuoys_qualitycontrol as bb_qc
import backyardbuoys_dataaccess as bb_da
import backyardbuoys_build_metadata as bb_meta
import backyardbuoys_generate_xml as bb_xml



from importlib import reload

# Constants
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'  # ISO 8601 format for timestamps
LOG_DATETIME_FORMAT = '%Y-%b-%d %H:%M:%S'  # Format for log messages

# # General Functions

# In[ ]:


def load_existing_netcdf(loc_id):
    
    # Get the directory containing the data
    basedir = bb.get_datadir()
    datadir = os.path.join(basedir, loc_id)
    if os.path.exists(os.path.join(basedir, loc_id + '_smart')):
        smartdir = os.path.join(basedir, loc_id + '_smart') 
    else:
        smartdir = None
    
    # Initialize a flag, indicating if their is
    # older data than the data returned
    # (i.e., data is pulled from the most recent
    #  month of data, but there are also prior months)
    olderFlag = False
    
    
    # Check the base data directories
    if not(os.path.exists(datadir)):
        print('No data file exists')
        ds = None
    else:
    
        # Create a file list of all the existing netCDFs
        # that correspond to the datafile
        # Exclude smart mooring files (those with '_smart' in the filename)
        datafiles = sorted([ii for ii in os.listdir(datadir) 
                            if (loc_id in ii) and ('.nc' in ii) and ('_smart' not in ii)])

        # Extract out the data year for all the files
        dataperiods = [ii[3+len(loc_id)+1:ii.find('.nc')]
                       for ii in datafiles]
        datayears = [int(ii[:4]) for ii in dataperiods]
        datamonths = [int(ii[4:]) for ii in dataperiods]

        # Find the most recent year, with a maximum prior
        # year being one year before the current year
        curyear = datetime.datetime.now().year
        validfiles = [year >= curyear-1 for year in datayears]
        if any(validfiles):
            datafiles = [datafiles[ii] for ii in np.where(validfiles)[0]]
            datayears = [datayears[ii] for ii in np.where(validfiles)[0]]
            datamonths = [datamonths[ii] for ii in np.where(validfiles)[0]]
            
            datadates = [datetime.datetime(datayears[ii], datamonths[ii], 1)
                         for ii in range(0,len(datayears))]
            
            maxind = np.argmax(datadates)
            lastfile = datafiles[maxind]
            if maxind > 0:
                olderFlag = True

            print('Loading in data from "' + lastfile + '"')
            ds = xr.load_dataset(os.path.join(datadir, lastfile))
            
            dimnames = [ii for ii in ds.dims]
            varnames = [ii for ii in ds.data_vars]
            if (len(dimnames) == 0) and (len(varnames) == 0):
                print('ERROR! Old data file has no dimensions or variables!')
                print('Do not load in this data file.')
                ds = None
        else:
            print('No data file exists')
            ds = None
            
    
    
    
    # Check the base data directories
    if smartdir is None:
        # Check if smart files exist in the main directory
        if os.path.exists(datadir):
            smart_files = [ii for ii in os.listdir(datadir) 
                          if (loc_id in ii) and ('.nc' in ii) and ('_smart' in ii)]
            if len(smart_files) > 0:
                smartdir = datadir
            else:
                ds_smart = None
        else:
            ds_smart = None
    
    if smartdir is not None:
    
        # Create a file list of all the existing netCDFs
        # that correspond to the smart mooring datafile
        datafiles = sorted([ii for ii in os.listdir(smartdir) 
                            if (loc_id in ii) and ('.nc' in ii) and ('_smart' in ii)])

        # Extract out the data year for all the files
        # Smart files are named like: bb_<loc_id>_smart_<YYYYMM>.nc
        # So we need to skip past 'bb_', loc_id, and '_smart_' to get the period
        dataperiods = [ii[3+len(loc_id)+7:ii.find('.nc')]
                       for ii in datafiles]
        datayears = [int(ii[:4]) for ii in dataperiods]
        datamonths = [int(ii[4:]) for ii in dataperiods]

        # Find the most recent year, with a maximum prior
        # year being one year before the current year
        curyear = datetime.datetime.now().year
        validfiles = [year >= curyear-1 for year in datayears]
        if any(validfiles):
            datafiles = [datafiles[ii] for ii in np.where(validfiles)[0]]
            datayears = [datayears[ii] for ii in np.where(validfiles)[0]]
            datamonths = [datamonths[ii] for ii in np.where(validfiles)[0]]
            
            datadates = [datetime.datetime(datayears[ii], datamonths[ii], 1)
                         for ii in range(0,len(datayears))]
            
            maxind = np.argmax(datadates)
            lastfile = datafiles[maxind]
            if maxind > 0:
                olderFlag = True

            print('Loading in data from "' + lastfile + '"')
            ds_smart = xr.load_dataset(os.path.join(smartdir, lastfile))
            
            dimnames = [ii for ii in ds.dims]
            varnames = [ii for ii in ds.data_vars]
            if (len(dimnames) == 0) and (len(varnames) == 0):
                print('ERROR! Old smartdata file has no dimensions or variables!')
                print('Do not load in this data file.')
                ds_smart = None
        else:
            print('No smartdata file exists')
            ds_smart = None
            
    
    return ds, ds_smart, olderFlag


# # Data processing function

# In[ ]:


def get_data_by_location(location_id, vars_to_get = 'ALL', 
                         time_start=None, time_end=None):
    
    ####################################
    # Pull data for for a given location
    location_data = bb_da.bbapi_get_location_data(location_id, vars_to_get, 
                                                  time_start, time_end)
    if (location_data is None) or (len(location_data) == 0):
        print('No data pulled.')
        return None, None
    
    ###############################################
    # Use location data to build a pandas dataframe
    
    def append_newvar(tot_dat, test_dat):
        
        # This function is used to append on each new variable 
        # to the total dataframe
        
        if test_dat is None:
            return tot_dat
        elif (tot_dat is None) and (test_dat is not None):
            # Return the test data as the initial dataset
            return test_dat.copy()
        
        # Match timestamps between existing data and new variable data
        # This ensures variables from the same observation are aligned
        basematchinds = []  # Indices in the existing total_data
        matchinds = []      # Indices in the new test_dat
        nonmatchinds = []   # Indices in test_dat that don't match total_data
        
        if len(test_dat) > 1:
            # Check each new data point against existing data
            for ii in range(0,len(test_dat)):
                # Look for matching point ID and platform ID
                if any(np.logical_and(tot_dat.pt_id == 
                                      test_dat.pt_id.to_numpy().squeeze()[ii],
                                      tot_dat.platform_id == 
                                      test_dat.platform_id.to_numpy().squeeze()[ii])):

                    # Found a match - record both indices
                    basematchinds.append(np.where(np.logical_and(tot_dat.pt_id == 
                                                                 test_dat.pt_id.to_numpy().squeeze()[ii],
                                                                 tot_dat.platform_id == 
                                                                 test_dat.platform_id.to_numpy().squeeze()[ii]))[0][0])
                    matchinds.append(ii)
                else:
                    # No match found - this is new data
                    nonmatchinds.append(ii)


            # Handle data merging based on matching results
            if len(matchinds) == len(tot_dat):
                # Perfect match - all timestamps align, just add the variable column
                tot_dat.loc[basematchinds,varname] = test_dat.loc[matchinds,varname].to_numpy().squeeze()
            else:
                # Partial or no match - need to merge carefully

                # First, add matching data points to their correct locations
                if len(matchinds) > 0:
                    tot_dat.loc[basematchinds,varname] = test_dat.loc[matchinds,varname].to_numpy().squeeze()

                # Then append non-matching data as new rows
                # Fill missing columns with NaNs
                if len(nonmatchinds) > 0:
                    # Create the variable column if it doesn't exist yet
                    if varname not in tot_dat.columns:
                        print('   ...Variable does not yet exist: ', varname)
                        tot_dat.loc[:,varname] = np.nan*np.ones(len(tot_dat))
                    # Concatenate the non-matching rows
                    tot_dat = pd.concat([tot_dat, 
                                         test_dat.iloc[nonmatchinds,:]]).reset_index(drop=True)
                    print('   ...Some mismatched for ' + varname + ': ' + str(len(nonmatchinds)) + ' points') 


        elif len(test_dat) == 1:
            tot_dat.loc[0,varname] = test_dat.loc[0,varname]

        return tot_dat
    
    # Extract variable data from location data and organize by depth
    # Surface measurements (depth=0) go to varnames
    # Subsurface measurements (depth≠0) go to smartvars (smart mooring sensors)
    loc_data_topds = {}
    varnames = []        # Surface wave buoy variables
    smartvars = []       # Smart mooring sensor variables
    smartdepths = []     # Depths of smart mooring sensors
    
    for varname in location_data.keys():
        loc_data_topds[varname] = {}
        loc_data_topds[varname]['units'] = location_data[varname]['units']
        data_colnames = [ii for ii in location_data[varname]['data'].keys()]
        temp_pd = pd.DataFrame(data = location_data[varname]['data'],
                               columns=data_colnames)
        # Rename 'value' column to the variable name for clarity
        data_colnames = [varname if ii == 'value' else ii for ii in data_colnames]
        loc_data_topds[varname]['data'] = temp_pd.rename(columns={"value": varname})
        
        # Categorize variables by depth
        if any(loc_data_topds[varname]['data']['depth']==0):
            varnames.append(varname)  # Surface measurement
        if any(loc_data_topds[varname]['data']['depth']!=0):
            smartvars.append(varname)  # Subsurface measurement
            smartdepths.append(np.unique(loc_data_topds[varname]['data']['depth'])[0])
    
    # Initialize separate dataframes for surface and subsurface data
    # Surface data: Wave buoy measurements (depth = 0)
    # Smart data: Smart mooring sensors (depth ≠ 0)
    if len(varnames) > 0:
        # Start with the first surface variable
        test_data = loc_data_topds[varnames[0]]['data'].copy()
        total_data = test_data[test_data['depth'] == 0].reset_index(drop=True)

        # Create a unique "point ID" for each observation
        # Used to match data points across different variables
        total_data['pt_id'] = [str(total_data['timestamp'][ii]) 
                               for ii in range(0,len(total_data))]
    else:
        total_data = None

    if len(smartvars) > 0:
        # Start with the first smart mooring variable
        test_data = loc_data_topds[smartvars[0]]['data'].copy()
        smart_data = test_data[test_data['depth'] != 0].reset_index(drop=True)

        # Create a unique "point ID" combining timestamp and depth
        smart_data['pt_id'] = [str(smart_data['timestamp'][ii]) 
                               for ii in range(0,len(smart_data))]
    else:
        smart_data = None

        
    
    # Step through all additional variables, and append them
    # to the total data dataframes
    for varname in varnames[1:]:
        # Extract out the data
        alltest_data = loc_data_topds[varname]['data'].copy()
        
        # Create a "point ID", created as a combination of
        # data timestamp and depth
        alltest_data['pt_id'] = [str(alltest_data['timestamp'][ii]) 
                                 for ii in range(0,len(alltest_data))]
        
        # Take each variables data, and put it into a dataframe
        if any(alltest_data != 0):
            test_data = alltest_data[alltest_data['depth'] == 0].reset_index(drop=True)
            smarttest_data = alltest_data[alltest_data['depth'] != 0].reset_index(drop=True)
            if smarttest_data.shape[0] == 0:
                smarttest_data = None  
        else:
            test_data = alltest_data.copy()
            smarttest_data = None
            
        total_data = append_newvar(total_data, test_data)
        if smarttest_data is not None:
            smart_data = append_newvar(smart_data, smarttest_data)
            
            
    # Step through all additional variables, and append them
    # to the total data dataframes
    for varname in smartvars[1:]:
        # Extract out the data
        alltest_data = loc_data_topds[varname]['data'].copy()
        
        # Create a "point ID", created as a combination of
        # data timestamp and depth
        alltest_data['pt_id'] = [str(alltest_data['timestamp'][ii]) 
                                 for ii in range(0,len(alltest_data))]
        
        # Take each variables data, and put it into a dataframe
        if any(alltest_data != 0):
            smarttest_data = alltest_data[alltest_data['depth'] != 0].reset_index(drop=True)
        else:
            smarttest_data = None
            
        if smarttest_data is not None:
            smart_data = append_newvar(smart_data, smarttest_data)
        
        
        
        

            
    
    # Convert Unix timestamps to datetime objects
    # Use vectorized pandas operation for efficiency
    total_data_time = total_data.timestamp.to_numpy()
    if len(total_data_time) > 1:
        # Vectorized conversion is much faster than list comprehension
        total_data['time'] = pd.to_datetime(total_data.timestamp, unit='s', errors='coerce')
    else:
        total_data['time'] = pd.to_datetime(total_data.timestamp, unit='s')
    
    # Drop any data with bad "time" values
    total_data = total_data.dropna(subset='time').reset_index(drop=True)
    
    if smart_data is not None:
        # Convert Unix timestamps to datetime objects using vectorized operation
        total_data_time = smart_data.timestamp.to_numpy()
        if len(total_data_time) > 1:
            smart_data['time'] = pd.to_datetime(smart_data.timestamp, unit='s', errors='coerce')
        else:
            smart_data['time'] = pd.to_datetime(smart_data.timestamp, unit='s')

        # Drop any data with bad "time" values
        smart_data = smart_data.dropna(subset='time').reset_index(drop=True)
        
    
    # For any duplicate point IDs, combine the values,
    # and drop the duplicate
    #if any(total_data.duplicated(subset='pt_id')):
    #    dupinds = np.where(total_data.duplicated(subset='pt_id'))[0]
    #    for dupind in dupinds:
    #        if dupind < len(total_data)-1:
    #            total_data.iloc[dupind,:].combine_first(total_data.iloc[dupind+1,:])
    #    total_data = total_data.drop_duplicates(subset=['pt_id'],keep='first').reset_index(drop=True)
            
    
    return total_data, smart_data


# In[ ]:


def check_for_necessary_variables(df, smartflag=False):
    
    if smartflag:
        df_names = ['lat',
                    'lon',
                    'depth']
    else:
        df_names = ['lat',
                    'lon',
                    'WaveHeightSig',
                    'WavePeriodMean',
                    'WaveDirMean',
                    'WaveDirMeanSpread',
                    'WavePeriodPeak',
                    'WaveDirPeak',
                    'WaveDirPeakSpread',
                    'WaterTemp'
                   ]
    
    npts = len(df)
    empty_col = np.nan*np.ones(npts)
    for col in df_names:
        if col not in df.columns:
            df[col] = empty_col
            
    return df


# In[ ]:


def rename_dataframe_columns(df, smartflag=False):
    
    if smartflag:
        df_names = ['lat',
                    'lon',
                    'depth']
        
        standard_names = ['latitude',
                          'longitude',
                          'depth'
                         ]
        
        df_cols = df.columns
        if 'WaterTemp' in df_cols:
            df_names.append('WaterTemp')
            standard_names.append('sea_water_temperature')
            
    else:
        df_names = ['lat',
                    'lon',
                    'WaveHeightSig',
                    'WavePeriodMean',
                    'WaveDirMean',
                    'WaveDirMeanSpread',
                    'WavePeriodPeak',
                    'WaveDirPeak',
                    'WaveDirPeakSpread',
                    'WaterTemp'
                   ]
    
        standard_names = ['latitude',
                          'longitude',
                          'sea_surface_wave_significant_height',
                          'sea_surface_wave_mean_period',
                          'sea_surface_wave_from_direction',
                          'sea_surface_wave_directional_spread',
                          'sea_surface_wave_period_at_variance_spectral_density_maximum',
                          'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
                          'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
                          'sea_surface_temperature'
                         ]
    
    rename_dict = {}
    for ii in range(0,len(df_names)):
        rename_dict[df_names[ii]] = standard_names[ii]
      
    try:
        df2 = df.rename(columns=rename_dict)
    except Exception as e:
        print('An error occurred in renaming the dataframe.')
        print(e)
        
    return df2


# In[ ]:


def get_buoy_qcflags(ds, loc_id, smartflag=False):
    
    dsnew_qcversion = ds.copy()
    if not(smartflag):
        # Convert period data into frequency for running the QC tests
        dsnew_qcversion['sea_surface_wave_frequency_at_variance_spectral_density_maximum'] = 1 / dsnew_qcversion['sea_surface_wave_period_at_variance_spectral_density_maximum'].values
        dsnew_qcversion['sea_surface_wave_mean_frequency'] = 1 / dsnew_qcversion['sea_surface_wave_mean_period'].values

    # Run QARTOD tests on the data
    if smartflag:
        smart_vars = ds.keys()
        dropvars = ['depth', 'latitude', 'longitude', 
                    'platform_id', 'timestamp', 
                    'type', 'pt_id', 'time']
        for var in dropvars:
            smart_vars = smart_vars.drop(var)
        qc_limits = bb_qc.load_all_smart_qc_limits(loc_id, [var for var in smart_vars])
    else:
        qc_limits = bb_qc.load_all_qc_limits(loc_id)
        
    ds_qc = bb_qc.process_qartod_tests(dsnew_qcversion, dsnew_qcversion.columns,
                                       qc_limits, smartflag)
    
    return ds_qc


# In[ ]:


def process_newdata(loc_id, rebuild_flag=False, rerun_tests=False):
    
    # Get location info from the metadata info json
    basedir = bb.get_datadir()
    sourcedir = os.path.join(basedir, loc_id, 'metadata')
    infodir = os.path.join(sourcedir, loc_id +'_info.json')

    # If the info path already exists, 
    # load in the info json, and update the relevant fields
    if os.path.exists(infodir):
        with open(infodir, 'r') as info_json:
            infodict = json.load(info_json)
    else:
        infodict = None

    check_spotters = False
    if (infodict is not None) and ('spotter_data' in infodict.keys()):        
        spotter_list = []
        valid_spotters = []
        # Filter out empty strings from spotter list
        spotter_list = [ii.strip() for ii in infodict['spotter_ids'].split(',') if ii.strip()]    
        for spotter in spotter_list:
            if ((infodict['spotter_data'][spotter]['can_data_archive'] == 'yes')
                and
                (infodict['spotter_data'][spotter]['can_share_ndbc_nws'] == 'yes')):
                valid_spotters.append(spotter)
        check_spotters = True

    
    # Load in existing data, if it exists
    if not(rebuild_flag):
        ds_old, ds_smart_old, olderFlag = load_existing_netcdf(loc_id)
        if ds_old is not None:
            ds_old = ds_old.sortby('time')
        if ds_smart_old is not None:
            ds_smart_old = ds_smart_old.sortby('time')
    else:
        ds_old = None
        ds_smart_old = None
    
    # If there is any existing data, get the last time stamp
    if (ds_old is not None) and not(rebuild_flag):
        # Extract out the first and last date
        # of the existing dataset
        firsttime = pd.Timestamp(ds_old['time'].data[0]).to_pydatetime()
        lasttime = pd.Timestamp(ds_old['time'].data[-1]).to_pydatetime()
        if (ds_smart_old is not None):
            first_smarttime = pd.Timestamp(ds_smart_old['time'].data[0]).to_pydatetime()
            last_smarttime = pd.Timestamp(ds_smart_old['time'].data[-1]).to_pydatetime()
            if last_smarttime < lasttime:
                lasttime = last_smarttime
            
        print('   Last time stamp of the existing data: ' + 
              lasttime.strftime(DATETIME_FORMAT))
        
        
        # Pull data since the later of the start of the data record,
        # or the start of the day prior to the day of the latest data
        if olderFlag:
            firstshift = datetime.timedelta(hours=12)
        else:
            firstshift = datetime.timedelta(hours=0)
            
        pulltime = np.max([firsttime-firstshift,
                           (lasttime.replace(hour=0,minute=0,second=0,microsecond=0) 
                            - datetime.timedelta(hours=12))])
    elif rebuild_flag:
        pulltime = datetime.datetime(2022,6,1)
        ds_old = None
        ds_smart_old = None
    else:
        pulltime = None
        ds_old = None
        ds_smart_old = None
    
    # Load in the data from the Backyard Buoys data API
    # Note, that for now, nothing is done with the smart mooring data (i.e., "ds_smart")
    if pulltime is not None:
        print('   ' + datetime.datetime.now().strftime(LOG_DATETIME_FORMAT) + 
              ': Pull data since ' + pulltime.strftime(DATETIME_FORMAT))
        pulltime = pulltime.strftime(DATETIME_FORMAT)
    else:
        print('   ' + datetime.datetime.now().strftime(LOG_DATETIME_FORMAT) + 
              ': Pull data since the beginning of the data record.')
        print(' Data record begins at ' + pulltime.strftime(DATETIME_FORMAT))
    ds, ds_smart = get_data_by_location(loc_id, time_start=pulltime)
    if ds is None:
        print('   Return without processing any data')
        return None, None
    print('   ' + datetime.datetime.now().strftime(LOG_DATETIME_FORMAT) + 
              ': Data pulled')
    
    
    #####################
    # Spotter Data only #
    #####################
    
    
    # Ensure that only data from spotters that has been authorized
    # is included for archiving
    if check_spotters:
        unique_spots = np.unique(ds.loc[:,'platform_id']).tolist()
        if any([spot not in valid_spotters for spot in unique_spots]):
            drop_spotters = [unique_spots[ii] for ii in
                             np.where([spot not in valid_spotters 
                                       for spot in unique_spots])[0]]
            for spotter in drop_spotters:
                print('   Data for ' + spotter + ' is not authorized to be archived.')
                print('   Drop this data from the dataset.')
                ds.drop(index=ds[ds['platform_id']==spotter].index).reset_index(drop=True)
            if len(ds) == 0:
                print('   No data remains to process. Return without processing any data.')
                return None, None
    
    # Check that the dataset has all the necessary columns
    ds = check_for_necessary_variables(ds)
    
    # Rename the data columns
    ds = rename_dataframe_columns(ds)

    # Run the QARTOD checks on the data
    ds_qc = get_buoy_qcflags(ds, loc_id)
    
    # Combine all the data together
    ds_df = pd.concat([ds, ds_qc], axis=1)
    
    
    #####################################################
    # Convert the pandas dataframe into an xarray dataset
    
    ds_xr = ds_df.copy()
    for col in ['platform_id','depth','timestamp', 
                'type','SeaSurfaceCondition',
                'WindSpeed','WindDirection','BarometricPressure']:    
        if col in ds_xr.columns:
            ds_xr = ds_xr.drop(columns=col)
    ds_xr = ds_xr.set_index('time').to_xarray()
        
    # Ensure that the "qartod" variables are "qc" variables
    for varname in list(ds_xr.keys()):
        if '_qartod_' in varname:
            new_varname = varname.replace('_qartod_','_qc_')
            ds_xr = ds_xr.rename({varname: new_varname})
            
    # Add the spotter buoy id as a variable
    ds_xr = ds_xr.assign(buoy_id=('time', ds_df['platform_id']))
            
    
    # Expand the dimensions to include location id, and sort by time
    ds_xr = ds_xr.expand_dims(dim={"location_id":[loc_id]}, axis=0).sortby('time')
    
    # If the dataset has "older" data (i.e., data from
    # earlier than the month of data loaded in), then
    # subset the data down to just the data since the
    # start of that file
    if (ds_old is not None) and olderFlag:
        ds_xr = ds_xr.where(ds_xr['time'] >= np.datetime64(firsttime), drop=True)
    
    
    
    ##################################################
    # Add the new data onto the existing data 
    if ds_old is not None:
        print('   Concat the datasets together')
        print('      Old dataset range: ' +  
              pd.Timestamp(ds_old['time'].data[0]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ') + 
             ' - ' + pd.Timestamp(ds_old['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ'))
        print('      New dataset range: ' +  
              pd.Timestamp(ds_xr['time'].data[0]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ') + 
             ' - ' + pd.Timestamp(ds_xr['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ'))
        print('      Old dataset size: ' + str(int(ds_old.sizes['time'])))
        print('      New dataset size: ' + str(int(ds_xr.sizes['time'])))
        
        ds_all = xr.concat([ds_old, ds_xr.sortby('time')], dim='time').sortby('time')
        print('      Merged dataset range: ' +  
              pd.Timestamp(ds_all['time'].data[0]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ') + 
             ' - ' + pd.Timestamp(ds_all['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ'))
        print('      Merged dataset size: ' + str(int(ds_all.sizes['time'])))
    else:
        ds_all = ds_xr.sortby('time').copy()
        
        
    
    ############################
    # Smart Mooring Processing #
    ############################
    
    if ds_smart is not None:    
        print('   Process smart mooring datasets.')
    
        # Ensure that only data from spotters that has been authorized
        # is included for archiving
        if check_spotters:
            unique_spots = np.unique(ds.loc[:,'platform_id']).tolist()
            if any([spot not in valid_spotters for spot in unique_spots]):
                drop_spotters = [unique_spots[ii] for ii in
                                 np.where([spot not in valid_spotters 
                                           for spot in unique_spots])[0]]
                for spotter in drop_spotters:
                    print('   Data for ' + spotter + ' is not authorized to be archived.')
                    print('   Drop this data from the dataset.')
                    ds.drop(index=ds[ds['platform_id']==spotter].index).reset_index(drop=True)
                if len(ds) == 0:
                    print('   No data remains to process. Return without processing any data.')
                    return None, None

        # Check that the dataset has all the necessary columns
        ds_smart = check_for_necessary_variables(ds_smart, smartflag=True)

        # Rename the data columns
        ds_smart = rename_dataframe_columns(ds_smart, smartflag=True)

        # Run the QARTOD checks on the data
        ds_smart_qc = get_buoy_qcflags(ds_smart, loc_id, smartflag=True)

        # Combine all the data together
        ds_smart_df = pd.concat([ds_smart, ds_smart_qc], axis=1)


        #####################################################
        # Convert the pandas dataframe into an xarray dataset

        ds_smart_xr = ds_smart_df.copy()
        for col in ['platform_id','timestamp']:    
            if col in ds_smart_xr.columns:
                ds_smart_xr = ds_smart_xr.drop(columns=col)
        ds_smart_xr = ds_smart_xr.set_index('time').to_xarray()

        # Ensure that the "qartod" variables are "qc" variables
        for varname in list(ds_smart_xr.keys()):
            if '_qartod_' in varname:
                new_varname = varname.replace('_qartod_','_qc_')
                ds_smart_xr = ds_smart_xr.rename({varname: new_varname})

        # Add the spotter buoy id as a variable
        ds_smart_xr = ds_smart_xr.assign(buoy_id=('time', ds_smart_df['platform_id']))


        # Expand the dimensions to include location id, and sort by time
        ds_smart_xr = ds_smart_xr.expand_dims(dim={"location_id":[loc_id]}, 
                                              axis=0).sortby('time')

        # If the dataset has "older" data (i.e., data from
        # earlier than the month of data loaded in), then
        # subset the data down to just the data since the
        # start of that file
        if (ds_smart_old is not None) and olderFlag:
            ds_smart_xr = ds_smart_xr.where(ds_smart_xr['time'] 
                                            >= np.datetime64(firsttime), drop=True)



        ##################################################
        # Add the new data onto the existing data 
        if ds_smart_old is not None:
            print('   Concat the smart datasets together')
            print('      Old dataset range: ' +  
                  pd.Timestamp(ds_smart_old['time'].data[0]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ') + 
                 ' - ' + 
                  pd.Timestamp(ds_smart_old['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ'))
            print('      New dataset range: ' +  
                  pd.Timestamp(ds_smart_xr['time'].data[0]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ') + 
                 ' - ' + 
                  pd.Timestamp(ds_smart_xr['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ'))
            print('      Old dataset size: ' + str(int(ds_old.sizes['time'])))
            print('      New dataset size: ' + str(int(ds_xr.sizes['time'])))

            ds_smart_all = xr.concat([ds_smart_old, 
                                      ds_smart_xr.sortby('time')], 
                                     dim='time').sortby('time')
            print('      Merged dataset range: ' +  
                  pd.Timestamp(ds_smart_all['time'].data[0]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ') + 
                 ' - ' + 
                  pd.Timestamp(ds_smart_all['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ'))
            print('      Merged dataset size: ' + str(int(ds_smart_all.sizes['time'])))
        else:
            ds_smart_all = ds_smart_xr.sortby('time').copy()
            
    else:
        ds_smart_all = None

        
        
    
    return ds_all, ds_smart_all


# In[ ]:


def rerun_qc_tests(ds_xr, loc_id, smartflag=False):
    
    # Make a copy of the xarray dataset
    ds_rerun = ds_xr.to_dataframe().reset_index()
    
    # Rerun the QC flagging on the dataset
    ds_qc = get_buoy_qcflags(ds_rerun, loc_id, smartflag)
    
    # Update the results in the xarray 
    # dataset for each qc test
    for col in ds_qc.columns:
        ds_xr[col.replace('qartod','qc')].loc[ds_xr['location_id'].data[0],:] = ds_qc.loc[:,col]
        
    return ds_xr


# In[ ]:


def check_duplicates(ds_all):
    
    ds_time = [pd.Timestamp(ii).to_pydatetime() 
               for ii in ds_all.sortby('time').variables['time'].data]

    print('   Checking for duplicates...')
    if np.any([np.diff(ds_time) == datetime.timedelta(seconds=0)]):
        dupinds = np.where([ii == datetime.timedelta(seconds=0) for ii in np.diff(ds_time)])[0]
        print('      Duplicates found to merge... # of duplicates: ' + str(len(dupinds)))

        ds_all_nodups = ds_all.copy().drop_duplicates(dim='time', keep='last')
        print('      Original dataset size: ', ds_all.sizes['time'])
        print('      Cleaned dataset size:  ', ds_all_nodups.sizes['time'])

        if ds_all_nodups.sizes['time'] + len(dupinds) < ds_all.sizes['time']:
            print('************************************************')
            print('*** *** ALERT! EXTRA DATA WAS DROPPED!!! *** ***')
            print('************************************************')

        return ds_all_nodups
    else:
        print('   No duplicates. were found in the merge.')
        return ds_all


# # NetCDF writing functions

# In[ ]:


def get_location_metadata(loc_id):
    
    # Load in the meta data for all locations
    basedir = bb.get_datadir()
    pathdir = os.path.join(basedir, loc_id, 'metadata', loc_id+'_metadata.json')
    
    if not(os.path.exists(pathdir)):
        return None
    else:
        with open(pathdir) as meta_json:
            meta = json.load(meta_json)
    
        
    return meta['metadata']


# In[ ]:


def netcdf_add_global_metadata(dataset, loc_id, smartflag=False):
    
    # Get metadata for a location
    meta = get_location_metadata(loc_id)
    
    dataset.creator_name = meta['creator_name']
    dataset.creator_email = meta['creator_email']
    dataset.creator_institution = meta['creator_institution']
    dataset.creator_type = 'institution'
    dataset.creator_url = meta['creator_url']
    dataset.creator_sector = meta['creator_type']
    dataset.creator_country = 'United States'

    dataset.publisher_name = 'Seth Travis'
    dataset.publisher_email = 'setht1@uw.edu'
    dataset.publisher_institution = 'Backyard Buoys'
    dataset.publisher_url = 'https://backyardbuoys.org/'
    dataset.publisher_type = 'institution'
    dataset.publisher_country = 'United States'

    if meta['contributor_name'] == '--':
        dataset.contributor_name = 'Backyard Buoys, NSF'
        dataset.contributor_url = 'https://backyardbuoys.org/, https://new.nsf.gov/funding/initiatives/convergence-accelerator/'
        dataset.contributor_role = 'publisher, funder'
    else:
        dataset.contributor_name = meta['contributor_name'] + ', Backyard Buoys, NSF'
        dataset.contributor_url = meta['contributor_url'] + ', https://backyardbuoys.org/, https://new.nsf.gov/funding/initiatives/convergence-accelerator/'
        dataset.contributor_role = meta['contributor_role'] + ', publisher, funder'
    dataset.contributor_role_vocabulary = 'https://vocab.nerc.ac.uk/collection/G04/current/'

    dataset_title = ('Backyard Buoys - ' + meta['ioos_association'] + ' - ' + meta['region'] 
                     + ': ' + meta['location_name'])
    if smartflag:
        dataset_title += ' (Smart Mooring)'
    dataset.title = dataset_title
    dataset.program = 'Backyard Buoys'
    dataset.program_url = 'https://backyardbuoys.org/'
    dataset.project = 'Backyard Buoys'
    dataset.summary = 'Surface wave and water conditions, as collected as part of the Backyard Buoys program'
    dataset.location_name = meta['location_name']
    dataset.location_id = meta['location_id']
    dataset.ioos_regional_association = meta['ioos_association']
    dataset.ioos_regional_association_url = meta['ioos_url']
    dataset.region = meta['region']
    
    dataset.platform = 'buoy'
    dataset.platform_vocabulary = "https://mmisw.org/ont/ioos/platform"
    dataset.platform_description = 'Sofar Spotter Buoy, moored'
    dataset.naming_authority = 'wmo'
    dataset.wmo_platform_code = meta['wmo_code']
    dataset.id = meta['wmo_code']
    dataset.gts_ingest = 'true'
    
    dataset.geospatial_lat_min = meta['southern_bound']
    dataset.geospatial_lat_max = meta['northern_bound']
    dataset.geospatial_lat_units = 'degrees_North'
    dataset.geospatial_lon_min = meta['western_bound']
    dataset.geospatial_lon_max = meta['eastern_bound']
    dataset.geospatial_lon_units = 'degrees_East'
    
    dataset.license = 'https://creativecommons.org/licenses/by-nc/4.0/deed.en'
    dataset.citation = (meta['creator_institution'] + '. ' 
                        + str(int(datetime.datetime.now().year)) + 
                        '. backyardbuoys_' + loc_id +'. Backyard Buoys. ' + 
                        'https://backyardbuoys.org/erddap/' + dataset.title)

    dataset.cdm_data_type = 'TimeSeries'
    dataset.cdm_timeseries_variables = 'location_id, latitude, longitude'
    dataset.subsetVariables = 'buoy_id'
    dataset.Conventions = 'CF-1.10, ACDD-1.3, IOOS-1.2'
    dataset.featureType = 'TimeSeries'
    dataset.institution = meta['creator_institution']
    
    dataset.history = 'Making the files'
    dataset.sourceUrl = 'https://data.backyardbuoys.org/'
    dataset.infoUrl = 'https://backyardbuoys.org/'
    dataset.keywords = 'buoy, direction, directional, earth, Earth Science &gt; Oceans &gt; Ocean Temperature &gt; Sea Surface Temperature, Earth Science &gt; Oceans &gt; Ocean Waves &gt; Significant Wave Height, Earth Science &gt; Oceans &gt; Ocean Waves &gt; Wave Period, Earth Science &gt; Oceans &gt; Ocean Waves &gt; Wave Spectra, Earth Science &gt; Oceans &gt; Ocean Waves &gt; Wave Speed/Direction, nsf, observing, ocean, oceans, period, sea_surface_temperature, sea_surface_wave_directional_spread, sea_surface_wave_directional_spread_at_variance_spectral_density_maximum, sea_surface_wave_from_direction, sea_surface_wave_from_direction_at_variance_spectral_density_maximum, sea_surface_wave_mean_period, sea_surface_wave_period_at_variance_spectral_density_maximum, sea_surface_wave_significant_height, surface, surface waves, watertemp, time, wave, waves'
    dataset.keywords_vocabulary = 'GCMD Science Keywords'
    dataset.standard_name_vocabulary = 'CF Standard Name Table v85'

    dataset.quality_control_method = 'https://ioos.noaa.gov/ioos-in-action/wave-data/'
    dataset.testOutOfDate = 'now-365days'
    dataset.creation_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    return dataset


# In[ ]:


def netcdf_add_variables(dataset, loc_id, ds):
    
    # Location platform Code
    platform = dataset.createVariable('location_id','S'+str(int(len(loc_id))),('location_id',))
    platform.long_name = 'location_id'
    platform.description = 'Backyard Buoys Location ID'
    platform.cf_role = 'timeseries_id'
    platform.units = '1'
    platform.ioos_category = 'Identifier'
    platform[0] = loc_id
    
    
    #######################
    # Identifying variables
    
    # Time
    datatime = dataset.createVariable('time','f8',('time'))
    datatime.standard_name = 'time'
    datatime.long_name = 'time'
    datatime.description = 'time of sampling'
    datatime.units = 'seconds since 1970-01-01 00:00:00'
    datatime.timezone = 'UTC'
    datatime.calendar = 'gregorian'
    datatime.gts_ingest = 'true'
    
    datadates = [pd.Timestamp(ii).to_pydatetime() for ii in ds['time'].data]
    reftime = datetime.datetime(1970,1,1)
    timestamp = [(ii-reftime).total_seconds() for ii in datadates]
    datatime[:] = timestamp
    
    # Spotter ID code
    spotter = dataset.createVariable('buoy_id','S11',('location_id','time'))
    spotter.long_name = 'buoy_id'
    spotter.description = 'Backyard Buoys Sofar Spotter Buoy ID'
    spotter.ioos_category = 'Identifier'
    spotter.units = '1'
    spotter.gts_ingest = 'false'
    spotter[:] = ds['buoy_id'][0].data
    
    # Latitude/longitude
    latitude = dataset.createVariable('latitude','f8',('location_id','time'))
    latitude.standard_name = 'latitude'
    latitude.long_name = 'latitude'
    latitude.description = 'Latitude'
    latitude.ioos_category = 'Location'
    latitude.units = 'degree_north'
    latitude.gts_ingest = 'true'
    latitude[:] = ds['latitude']
    
    longitude = dataset.createVariable('longitude','f8',('location_id','time'))
    longitude.standard_name = 'longitude'
    longitude.long_name = 'longitude'
    longitude.description = 'Longitude'
    longitude.ioos_category = 'Location'
    longitude.units = 'degree_east'
    longitude.gts_ingest = 'true'
    longitude[:] = ds['longitude']
    
    
    
    ##############################
    # Data variables
    ##############################
    
    
    def make_ancvar_str(varname):
        # Define the general QC flag suffixes
        qc_vars = ['qc_agg', 'qc_gross_range_test', 'qc_rate_of_change_test',
                   'qc_spike_test', 'qc_flat_line_test']
        # Initialize an ancillary variable stirng
        ancvar_str = ''
        for qc_var in qc_vars:
            # Add each qc flag specific to the variable to the string
            ancvar_str = ancvar_str + varname + '_' + qc_var + ' '
        # Remove the last ", " from the string
        ancvar_str = ancvar_str[:-1]
        
        return ancvar_str
        
    
    ###
    # Wave height
    
    waveheight = dataset.createVariable('sea_surface_wave_significant_height','f8',('location_id','time'))
    waveheight.standard_name = 'sea_surface_wave_significant_height'
    waveheight.long_name = 'sea_surface_wave_significant_height'
    waveheight.description = 'Significant Wave Height of Surface Waves'
    waveheight.ioos_category = 'Surface Waves'
    waveheight.units = 'm'
    waveheight.coverage_content_type = 'physicalMeasurement'
    waveheight.gts_ingest = 'true'
    waveheight.ancillary_variables = make_ancvar_str('sea_surface_wave_significant_height')
    waveheight[:] = ds['sea_surface_wave_significant_height']
    
    ###
    # Mean period waves
    
    meanperiod = dataset.createVariable('sea_surface_wave_mean_period','f8',('location_id','time'))
    meanperiod.standard_name = 'sea_surface_wave_mean_period'
    meanperiod.long_name = 'sea_surface_wave_mean_period'
    meanperiod.description = 'Mean Wave Period'
    meanperiod.ioos_category = 'Surface Waves'
    meanperiod.units = 's'
    meanperiod.coverage_content_type = 'physicalMeasurement'
    meanperiod.gts_ingest = 'true'
    meanperiod.ancillary_variables = make_ancvar_str('sea_surface_wave_mean_period')
    meanperiod[:] = ds['sea_surface_wave_mean_period']
    
    meanperiod_direction = dataset.createVariable('sea_surface_wave_from_direction','f8',('location_id','time'))
    meanperiod_direction.standard_name = 'sea_surface_wave_from_direction'
    meanperiod_direction.long_name = 'sea_surface_wave_from_direction'
    meanperiod_direction.description = 'Mean Wave Direction'
    meanperiod_direction.ioos_category = 'Surface Waves'
    meanperiod_direction.units = 'degree'
    meanperiod_direction.coverage_content_type = 'physicalMeasurement'
    meanperiod_direction.gts_ingest = 'true'
    meanperiod_direction.ancillary_variables = make_ancvar_str('sea_surface_wave_from_direction')
    meanperiod_direction[:] = ds['sea_surface_wave_from_direction']
    
    meanperiod_spread = dataset.createVariable('sea_surface_wave_directional_spread','f8',('location_id','time'))
    meanperiod_spread.standard_name = 'sea_surface_wave_directional_spread'
    meanperiod_spread.long_name = 'sea_surface_wave_directional_spread'
    meanperiod_spread.description = 'Mean Wave Directional Spread'
    meanperiod_spread.ioos_category = 'Surface Waves'
    meanperiod_spread.units = 'degree'
    meanperiod_spread.coverage_content_type = 'physicalMeasurement'
    meanperiod_spread.gts_ingest = 'true'
    meanperiod_spread.ancillary_variables = make_ancvar_str('sea_surface_wave_directional_spread')
    meanperiod_spread[:] = ds['sea_surface_wave_directional_spread']
    
    ###
    # Peak period waves
    
    peakperiod = dataset.createVariable('sea_surface_wave_period_at_variance_spectral_density_maximum','f8',('location_id','time'))
    peakperiod.standard_name = 'sea_surface_wave_period_at_variance_spectral_density_maximum'
    peakperiod.long_name = 'sea_surface_wave_period_at_variance_spectral_density_maximum'
    peakperiod.description = 'Peak Wave Period'
    peakperiod.ioos_category = 'Surface Waves'
    peakperiod.units = 's'
    peakperiod.coverage_content_type = 'physicalMeasurement'
    peakperiod.gts_ingest = 'true'
    peakperiod.ancillary_variables = make_ancvar_str('sea_surface_wave_period_at_variance_spectral_density_maximum')
    peakperiod[:] = ds['sea_surface_wave_period_at_variance_spectral_density_maximum']
    
    peakperiod_direction = dataset.createVariable('sea_surface_wave_from_direction_at_variance_spectral_density_maximum','f8',('location_id','time'))
    peakperiod_direction.standard_name = 'sea_surface_wave_from_direction_at_variance_spectral_density_maximum'
    peakperiod_direction.long_name = 'sea_surface_wave_from_direction_at_variance_spectral_density_maximum'
    peakperiod_direction.description = 'Peak Wave Direction'
    peakperiod_direction.ioos_category = 'Surface Waves'
    peakperiod_direction.units = 'degree'
    peakperiod_direction.coverage_content_type = 'physicalMeasurement'
    peakperiod_direction.gts_ingest = 'true'
    peakperiod_direction.ancillary_variables = make_ancvar_str('sea_surface_wave_from_direction_at_variance_spectral_density_maximum')
    peakperiod_direction[:] = ds['sea_surface_wave_from_direction_at_variance_spectral_density_maximum']
    
    peakperiod_spread = dataset.createVariable('sea_surface_wave_directional_spread_at_variance_spectral_density_maximum','f8',('location_id','time'))
    peakperiod_spread.standard_name = 'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum'
    peakperiod_spread.long_name = 'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum'
    peakperiod_spread.description = 'Peak Wave Directional Spread'
    peakperiod_spread.ioos_category = 'Surface Waves'
    peakperiod_spread.units = 'degree'
    peakperiod_spread.coverage_content_type = 'physicalMeasurement'
    peakperiod_spread.gts_ingest = 'true'
    peakperiod_spread.ancillary_variables = make_ancvar_str('sea_surface_wave_directional_spread_at_variance_spectral_density_maximum')
    peakperiod_spread[:] = ds['sea_surface_wave_directional_spread_at_variance_spectral_density_maximum']
    
    ###
    # Sea surface temperature
    
    seatemp = dataset.createVariable('sea_surface_temperature','f8',('location_id','time'))
    seatemp.standard_name = 'sea_surface_temperature'
    seatemp.long_name = 'sea_surface_temperature'
    seatemp.description = 'Sea Water Temperature at the Surface'
    seatemp.ioos_category = 'Temperature'
    seatemp.units = 'degrees_C'
    seatemp.coverage_content_type = 'physicalMeasurement'
    seatemp.gts_ingest = 'true'
    seatemp.ancillary_variables = make_ancvar_str('sea_surface_temperature')
    seatemp[:] = ds['sea_surface_temperature']
    
    
    
    #################
    # QARTOD Flags
    
    orig_varnames = ['significantWaveHeight', 
                     'meanPeriod', 'meanDirection', 'meanDirectionalSpread',
                     'peakPeriod', 'peakDirection', 'peakDirectionalSpread',
                     'waterTemp']
    varnames = ['sea_surface_wave_significant_height',
                'sea_surface_wave_mean_period',
                'sea_surface_wave_from_direction',
                'sea_surface_wave_directional_spread',
                'sea_surface_wave_period_at_variance_spectral_density_maximum',
                'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
                'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
                'sea_surface_temperature']
    varlabs = ['Significant Wave Height of Surface Waves',
               'Mean Wave Period', 'Mean Wave Direction', 'Mean Wave Directional Spread',
               'Peak Wave Period', 'Peak Wave Direction', 'Peak Wave Directional Spread',
               'Sea Surface Temperature']
    
    origqc_vars = ['qc_agg', 
                   'qc_gross_range_test', 'qc_rate_of_change_test',
                   'qc_spike_test', 'qc_flat_line_test']
    qc_vars = ['qc_agg', 
               'qc_gross_range_test', 'qc_rate_of_change_test',
               'qc_spike_test', 'qc_flat_line_test']
    qc_standards = ['aggregate_quality_flag', 
                    'gross_range_test_quality_flag', 
                    'rate_of_change_test_quality_flag', 'spike_test_quality_flag',
                    'flat_line_test_quality_flag']
    qc_labs = ['Aggregate Flag', 'Gross Range Test Flag', 
               'Rate of Change Test Flag', 'Spike Test Flag',
               'Flat Line Test Flag']
    
    for ii in range(0,len(varnames)):
        # Step through each QARTOD test for each variable, and write it to the file
        origvar = orig_varnames[ii]
        var = varnames[ii]
        varlabel = varlabs[ii]
        for jj in range(0,len(qc_vars)):
            origqc_var = origqc_vars[jj]
            qc_var = qc_vars[jj]
            qc_standard = qc_standards[jj]
            qc_lab = qc_labs[jj]
            
            # Create new variable
            temp_qcvar = dataset.createVariable(var + '_' + qc_var,'i4',('location_id','time'))
            temp_qcvar.standard_name = qc_standard
            temp_qcvar.long_name = var + '_' + qc_var
            temp_qcvar.description = varlabel + ' ' + qc_lab
            temp_qcvar.ioos_category = 'Quality Control'
            temp_qcvar.units = '1'
            temp_qcvar.coverage_content_type = 'qualityInformation'
            temp_qcvar.flag_vals = '1, 2, 3, 4, 9'
            temp_qcvar.flag_meanings = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            if qc_var == 'qc_agg':
                temp_qcvar.gts_ingest = 'true'
            temp_qcvar[:] = ds[var + '_' + origqc_var]
    
    
    return dataset


# In[ ]:


def netcdf_add_smart_variables(dataset, loc_id, ds_smart, smart_vars):
    
    # Location platform Code
    platform = dataset.createVariable('location_id','S'+str(int(len(loc_id))),('location_id',))
    platform.long_name = 'location_id'
    platform.description = 'Backyard Buoys Location ID'
    platform.cf_role = 'timeseries_id'
    platform.units = '1'
    platform.ioos_category = 'Identifier'
    platform[0] = loc_id
    
    
    #######################
    # Identifying variables
    
    # Time
    datatime = dataset.createVariable('time','f8',('time'))
    datatime.standard_name = 'time'
    datatime.long_name = 'time'
    datatime.description = 'time of sampling'
    datatime.units = 'seconds since 1970-01-01 00:00:00'
    datatime.timezone = 'UTC'
    datatime.calendar = 'gregorian'
    datatime.gts_ingest = 'true'
    
    datadates = [pd.Timestamp(ii).to_pydatetime() for ii in ds_smart['time'].data]
    reftime = datetime.datetime(1970,1,1)
    timestamp = [(ii-reftime).total_seconds() for ii in datadates]
    datatime[:] = timestamp
    
    # Spotter ID code
    spotter = dataset.createVariable('buoy_id','S11',('location_id','time'))
    spotter.long_name = 'buoy_id'
    spotter.description = 'Backyard Buoys Sofar Spotter Buoy ID'
    spotter.ioos_category = 'Identifier'
    spotter.units = '1'
    spotter.gts_ingest = 'false'
    spotter[:] = ds_smart['buoy_id'][0].data
    
    # Latitude/longitude
    latitude = dataset.createVariable('latitude','f8',('location_id','time'))
    latitude.standard_name = 'latitude'
    latitude.long_name = 'latitude'
    latitude.description = 'Latitude'
    latitude.ioos_category = 'Location'
    latitude.units = 'degree_north'
    latitude.gts_ingest = 'true'
    latitude[:] = ds_smart['latitude']
    
    longitude = dataset.createVariable('longitude','f8',('location_id','time'))
    longitude.standard_name = 'longitude'
    longitude.long_name = 'longitude'
    longitude.description = 'Longitude'
    longitude.ioos_category = 'Location'
    longitude.units = 'degree_east'
    longitude.gts_ingest = 'true'
    longitude[:] = ds_smart['longitude']
    
    # Depth
    
    depth = dataset.createVariable('depth','f8',('location_id','time'))
    depth.standard_name = 'depth'
    depth.long_name = 'depth'
    depth.description = 'Z-coordinate of observation in vertical distance below reference. Down is positive. (reference is sea surface)'
    depth.ioos_category = 'Location'
    depth.units = 'm'
    depth.positive = 'down'
    depth.gts_ingest = 'true'
    depth[:] = abs(ds_smart['depth'])
    
    
    
    ##############################
    # Data variables
    ##############################
    
    
    def make_ancvar_str(varname):
        # Define the general QC flag suffixes
        qc_vars = ['qc_agg', 'qc_gross_range_test', 'qc_rate_of_change_test',
                   'qc_spike_test', 'qc_flat_line_test']
        # Initialize an ancillary variable stirng
        ancvar_str = ''
        for qc_var in qc_vars:
            # Add each qc flag specific to the variable to the string
            ancvar_str = ancvar_str + varname + '_' + qc_var + ' '
        # Remove the last ", " from the string
        ancvar_str = ancvar_str[:-1]
        
        return ancvar_str
        
    
    
    ####################################
    # Step through each of the variables
    # taken from the smart sensors
    ####################################
    orig_varnames = []
    varnames = []
    varlabs = []
    for smartvar in smart_vars:
        
        if smartvar == 'sea_water_temperature':            
            # Water temperature
            temper = dataset.createVariable('sea_water_temperature','f8',('location_id','time'))
            temper.standard_name = 'sea_water_temperature'
            temper.long_name = 'sea_water_temperature'
            temper.description = 'Sea Water Temperature'
            temper.ioos_category = 'Temperature'
            temper.units = 'degrees_C'
            temper.coverage_content_type = 'physicalMeasurement'
            temper.gts_ingest = 'true'
            temper.ancillary_variables = make_ancvar_str('sea_water_temperature')
            temper[:] = ds_smart['sea_water_temperature']
            
            orig_varnames.append('waterTemp')
            varnames.append('sea_water_temperature')
            varlabs.append('Sea Water Temperature')

    
    
    
    #################
    # QARTOD Flags
    
    origqc_vars = ['qc_agg', 
                   'qc_gross_range_test', 'qc_rate_of_change_test',
                   'qc_spike_test', 'qc_flat_line_test']
    qc_vars = ['qc_agg', 
               'qc_gross_range_test', 'qc_rate_of_change_test',
               'qc_spike_test', 'qc_flat_line_test']
    qc_standards = ['aggregate_quality_flag', 
                    'gross_range_test_quality_flag', 
                    'rate_of_change_test_quality_flag', 'spike_test_quality_flag',
                    'flat_line_test_quality_flag']
    qc_labs = ['Aggregate Flag', 'Gross Range Test Flag', 
               'Rate of Change Test Flag', 'Spike Test Flag',
               'Flat Line Test Flag']
    
    for ii in range(0,len(varnames)):
        # Step through each QARTOD test for each variable, and write it to the file
        origvar = orig_varnames[ii]
        var = varnames[ii]
        varlabel = varlabs[ii]
        for jj in range(0,len(qc_vars)):
            origqc_var = origqc_vars[jj]
            qc_var = qc_vars[jj]
            qc_standard = qc_standards[jj]
            qc_lab = qc_labs[jj]
            
            # Create new variable
            temp_qcvar = dataset.createVariable(var + '_' + qc_var,'i4',('location_id','time'))
            temp_qcvar.standard_name = qc_standard
            temp_qcvar.long_name = var + '_' + qc_var
            temp_qcvar.description = varlabel + ' ' + qc_lab
            temp_qcvar.ioos_category = 'Quality Control'
            temp_qcvar.units = '1'
            temp_qcvar.coverage_content_type = 'qualityInformation'
            temp_qcvar.flag_vals = '1, 2, 3, 4, 9'
            temp_qcvar.flag_meanings = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            if qc_var == 'qc_agg':
                temp_qcvar.gts_ingest = 'true'
            temp_qcvar[:] = ds_smart[var + '_' + origqc_var]
    
    
    return dataset


# In[ ]:


def write_netcdf(ds, loc_id, datayear, datamonth, smart_vars=None):
    
    # If there is no data in the dataframe, do not make a netCDF
    if len(ds) == 0:
        print('No data exists in this file. Do not make a netCDF.')
        return
    
    smartflag = False
    if smart_vars is not None:
        smartflag = True
    
    # Get the base data directory
    basedir = bb.get_datadir()
    
    # Define the folder for the location id,
    # and if the folder does not exist,
    # make the directory
    datadir = os.path.join(basedir, loc_id)
    if not(os.path.exists(datadir)):
        os.mkdir(datadir)
    
    
    # Define the file name
    datayear_str = str(int(datayear))
    if datamonth < 10:
        datamonth_str = '0' + str(int(datamonth))
    else:
        datamonth_str = str(int(datamonth))
    tempfile = 'bb_tempfile.nc'
    if smartflag:
        newfile = 'bb_' + loc_id + '_smart_' + datayear_str + datamonth_str + '.nc'
    else:
        newfile = 'bb_' + loc_id + '_' + datayear_str + datamonth_str + '.nc'
    
    # Check the number of samples in the file
    nsamps = len(ds['time'])
    
    
    # Open a new netCDF file for writing
    dataset = Dataset(os.path.join(datadir, tempfile), 'w', format='NETCDF4')
    success_ncflag = False
    print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': ' + tempfile + ' open for writing...')
    
    try:
        # Write the global metadata for the netCDF
        dataset = netcdf_add_global_metadata(dataset, loc_id, smartflag)
        
        # Create the dimensions of the file
        dataset.createDimension('location_id',1)
        dataset.createDimension('time',nsamps)
        
        # Add the variables
        if smartflag: 
            netcdf_add_smart_variables(dataset, loc_id, ds, smart_vars)       
        else:
            netcdf_add_variables(dataset, loc_id, ds)
        success_ncflag = True
    except Exception as e:
        # If any errors occur, print out the error message
        print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': something went wrong')
        print(e)
        success_ncflag = False
        
    print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': writing ' + tempfile + ' complete')
    dataset.close()
    if success_ncflag:        
        ##################################################
        # Move and rename the netCDF
        # Note: this is done to ensure that the file
        #       is properly identified by the ERDDAP
        #       server as a viable file quickly, rather
        #       than needing a full dataset reload
        #       See here:
        #       https://coastwatch.pfeg.noaa.gov/erddap/download/setupDatasetsXml.html#updateEveryNMillis
        print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': Replace ' + tempfile + ' with ' + newfile)
        shutil.move(os.path.join(datadir,tempfile), 
                    os.path.join(datadir,newfile))
        print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': ' + newfile + ' successfully replaced.')
        
        
    
    return

def add_wmo_code_to_data(loc_id):
    # Get the path for the metadata info json
    basedir = bb.get_datadir()
    metadir = os.path.join(basedir, loc_id, 'metadata', loc_id +'_metadata.json')
    if not(os.path.exists(metadir)):
        print('   No metadata exists for project: ' + loc_id)
        print('   Try to make the meta data for project: ' + loc_id)
        meta_success = bb_meta.make_projects_metadata(loc_id)
        if not(meta_success):
            print('Unable to make the data file for this project')
            return False
    
    # Load in the metadata json
    with open(metadir, 'r') as meta_json:
        metadict = json.load(meta_json)
        
    # Extract out the WMO code
    wmo_code = metadict['metadata']['wmo_code']
    if wmo_code == '':
        print(f'No WMO code found for location {loc_id}. Cannot add to metadata.')
        return False
    
    # Add the WMO code to the metadata for all netCDF files
    ncfiles = [f for f in os.listdir(os.path.join(basedir, loc_id)) if f.endswith('.nc')]
    for ncfile in ncfiles:
        ncpath = os.path.join(basedir, loc_id, ncfile)
        dataset = Dataset(ncpath, 'a')
        dataset.wmo_platform_code = wmo_code
        dataset.id = wmo_code
        dataset.close()

    print(f'Added WMO code {wmo_code} to netCDFs for location {loc_id} metadata.')
    
    return True


# # Main Processing Functions

# In[ ]:


def update_data_by_location(loc_id, rebuild_flag=False, rerun_tests=False):
    
    basedir = bb.get_datadir()
    
    # Create and/or update the location info json
    addspotterFlag = update_location_info(loc_id, rebuild_flag)
    
    # If update_location_info returns False, the location has no recent data
    # and cannot be processed
    if (addspotterFlag is False) and not(rebuild_flag):
        print(f'{loc_id}: No recent data available. Cannot process location.')
        return False
    
    # Load in the meta data for all locations
    metadir = os.path.join(basedir, loc_id, 'metadata', loc_id +'_metadata.json')
    if not(os.path.exists(metadir)):
        print('   No metadata exists for project: ' + loc_id)
        print('   Try to make the meta data for project: ' + loc_id)
        meta_success = bb_meta.make_projects_metadata(loc_id, addspotterFlag)
        if not(meta_success):
            print('Unable to make the data file for this project')
            return False
    
    # Download any new data
    ds_all, ds_all_smart = process_newdata(loc_id, rebuild_flag)
    if ds_all is None:
        print('As there is no data, no netCDF is created. End the process, and move on.')
        return False
    
    # If need be, rerun all the QC tests
    if rerun_tests:
        ds_all = rerun_qc_tests(ds_all, loc_id)
        
    # Check all the data for duplicates
    ds_all = check_duplicates(ds_all.copy())
    
    # Group all the data by year
    ds_grouped = ds_all.groupby('time.year')
    
    # Loop through each unique year of data, 
    # and write the netcdf file for the
    # location ID for a given year
    print('     Years of data to write:', list(ds_grouped.groups.keys()))
    for year in ds_grouped.groups.keys():
        
        ds_subgrouped = ds_grouped[year].groupby('time.month')
        print('     Months of data to write:', list(ds_subgrouped.groups.keys()))
        for month in ds_subgrouped.groups.keys():
            # Write the netcdf of the file
            write_netcdf(ds_subgrouped[month].sortby('time'), 
                         loc_id, year, month)
            
            
    if ds_all_smart is not None:
        
        smart_vars = get_valid_smart_vars(ds_all_smart)
        
        # If need be, rerun all the QC tests
        if rerun_tests:
            ds_all_smart = rerun_qc_tests(ds_all_smart, loc_id, smartflag=True)

        # Check all the data for duplicates
        ds_all_smart = check_duplicates(ds_all_smart.copy())

        # Group all the data by year
        ds_smart_grouped = ds_all_smart.groupby('time.year')

        # Loop through each unique year of data, 
        # and write the netcdf file for the
        # location ID for a given year
        print('Smart Mooring data: ')
        print('Years of data to write:', list(ds_smart_grouped.groups.keys()))
        for year in ds_smart_grouped.groups.keys():

            ds_subgrouped = ds_smart_grouped[year].groupby('time.month')
            print('Months of data to write:', list(ds_subgrouped.groups.keys()))
            for month in ds_subgrouped.groups.keys():
                # Write the netcdf of the file
                write_netcdf(ds_subgrouped[month].sortby('time'), 
                             loc_id, year, month, smart_vars)
        
            
    
    
    return True


# In[ ]:


def get_valid_smart_vars(ds):
    
    valid_smart_vars = ['sea_water_temperature',
                        'sea_water_pressure']
    
    ds_keys = ds.keys()
    smart_vars = []
    for key in ds_keys:
        if any([key == var for var in valid_smart_vars]):
            smart_vars.append(key)
            
    return smart_vars


# In[ ]:


def update_location_info(loc_id, rebuild_flag=False):
    
    
    # Define the info json path
    basedir = bb.get_datadir()
    sourcedir = os.path.join(basedir, loc_id, 'metadata')
    infodir = os.path.join(sourcedir, loc_id +'_info.json')

    
    # If the info path already exists, 
    # load in the info json, and update the relevant fields
    if os.path.exists(infodir):
        with open(infodir, 'r') as info_json:
            infodict = json.load(info_json)
            
        # Extract out a list of all spotter IDs for a location
        # Filter out empty strings that may result from trailing commas or empty fields
        if ',' in infodict['spotter_ids']:
            spotter_list = [ii.strip() for ii in 
                            np.unique(infodict['spotter_ids'].split(',')) 
                            if ii.strip()]
        else:
            spotter_list = [infodict['spotter_ids']] if infodict['spotter_ids'] else []
           
        
        # Load in the location info
        bb_locs = bb_da.bbapi_get_locations(recentFlag=True)
        addspotterFlag = False
        if not(any([loc == loc_id for loc in bb_locs.keys()])) and not(rebuild_flag):
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
        bb_spots = bb_da.bbapi_get_platforms()
        spotter_liststr = ''
        spotter_data = {}
        for spotter in new_spotter_list:
            # Skip empty spotter IDs
            if not spotter or not spotter.strip():
                print(f'   Skipping empty spotter ID')
                continue
            
            if spotter not in bb_spots:
                print(f'   Warning: Spotter {spotter} not found in platform data')
                continue
                
            if spotter_liststr == '':
                extra_str = ''
            else:
                extra_str = ', '
            spotter_liststr += extra_str + spotter
            spotter_data[spotter] = bb_spots[spotter]
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
        bb_spots = bb_da.bbapi_get_platforms(allplatsFlag=True)

        spotter_liststr = ''
        spotter_data = {}
        for spotter in spotter_list:
            if spotter_liststr == '':
                extra_str = ''
            else:
                extra_str = ', '
            spotter_liststr += extra_str + spotter
            spotter_data[spotter] = bb_spots[spotter]
                
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


# In[ ]:


def update_all_locations(rebuild_flag=False, rerun_tests=False):
    
    # Get a list of the backyard buoys projects
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations()
    # Extract out the locaiton ID
    # if the location is an official Backyard Buoys
    # site (and not "Friends of ...")
    loc_ids = [bb_locs[ii]['loc_id'] for ii in bb_locs
               if (bb_locs[ii]['is_byb'] == 'yes')]
    
    # Identify which locations are active
    loc_active = [True if 
                  (bb_locs[ii]['status'] == 'active')
                  else False for ii in loc_ids]
    
    # If need be, try to create the metadata for each project
    missing_projs = []
    add_projs = []
    for ii in range(0,len(loc_ids)):
        pathdir = os.path.join(basedir, loc_ids[ii], 
                               'metadata', loc_ids[ii]+'_metadata.json')
        if not(os.path.exists(pathdir)):
            missing_projs.append(loc_ids[ii])
    add_projs = bb_meta.make_projects_metadata(missing_projs)
            
        
    # Step through each project, and update the data
    for ii in range(0,len(loc_ids)):

        if loc_active[ii] or rebuild_flag:
            # Load in the meta data for all locations
            pathdir = os.path.join(basedir, loc_ids[ii], 'metadata', loc_ids[ii]+'_metadata.json')
            if not(os.path.exists(pathdir)):
                print(loc_ids[ii] + ': No metadata exists for this project. Continue on')
                continue


            print('\n' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') 
                  + ': Processing data for ' + loc_ids[ii])
            update_success = update_data_by_location(loc_ids[ii], rebuild_flag, rerun_tests)
                
            if update_success:
                print(datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') 
                      + ': Data update complete\n')
            else:
                print(datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') 
                      + ': Data was not updated\n')
    
    # Add new projects, as found in the projects with new metadata,
    # add that data to the ERDDAP datasets
    if add_projs is not None:
        print('Adding ' + str(len(add_projs)) + ' new datasets to ERDDAP datasets.xml')
        bb_xml.update_datasets_xml(add_projs)
            
    return


# In[ ]:


def update_netcdf_metadata_by_location(loc_id):
    
    # Get the base data directory# Get the base data directory
    basedir = bb.get_datadir()
    datadir = os.path.join(basedir, loc_id)

    filelist = [ii for ii in os.listdir(datadir) if 
                (('.nc' in ii) and ('temp_ncfile' not in ii))]
    tmpfilepath = os.path.join(datadir, 'temp_ncfile.nc')

    for file in filelist:
        filepath = os.path.join(datadir, file)
        print(filepath)

        try:
            print('Make a copy of the netcdf')
            shutil.copy(filepath, tmpfilepath)

            print('Update netcdf metadata')
            dataset = Dataset(tmpfilepath, 'a')
            try:
                dataset = netcdf_add_global_metadata(dataset, loc_id)
            except Exception as e2:
                print('Error occured while updating metadata')
                print(e2)
            dataset.close()

            print('Replace the original with the updated copy.')
            shutil.move(tmpfilepath, filepath)
        except Exception as e:
            print('Error occurred while working with netcdf file.')
            print(e)
        print('\n\n')
        
    return

