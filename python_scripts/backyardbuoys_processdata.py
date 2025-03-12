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


# # General Functions

# In[ ]:


def load_existing_netcdf(loc_id):
    
    # Get the directory containing the data
    basedir = bb.get_datadir()
    datadir = os.path.join(basedir, loc_id)
    
    if not(os.path.exists(datadir)):
        print('No data file exists')
        ds = None
        
    else:
    
        # Create a file list of all the existing netCDFs
        # that correspond to the datafile
        datafiles = sorted([ii for ii in os.listdir(datadir) 
                            if (loc_id in ii) and ('.nc' in ii)])

        # Extract out the data year for all the files
        datayears = [int(ii[3+len(loc_id)+1:ii.find('.nc')]) 
                     for ii in datafiles]

        # Find the most recent year, with a maximum prior
        # year being one year before the current year
        curyear = datetime.datetime.now().year
        validfiles = [year >= curyear-1 for year in datayears]
        if any(validfiles):
            datafiles = [datafiles[ii] for ii in np.where(validfiles)[0]]
            datayears = [datayears[ii] for ii in np.where(validfiles)[0]]
            
            maxind = np.argmax(datayears)
            lastfile = datafiles[maxind]

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
    
    return ds


# # Data processing function

# In[ ]:


def get_data_by_location(location_id, vars_to_get = 'ALL', time_start=None, time_end=None):
    
    ###################################
    # Pull data for NANOOS Tables
    location_data = bb_da.bbapi_get_location_data(location_id, vars_to_get, time_start, time_end)
    if (location_data is None) or (len(location_data) == 0):
        print('No data pulled.')
        return None
    
    ###############################################
    # Use location data to build a pandas dataframe
    
    def append_newvar(tot_dat, test_dat):
        
        # This function is used to append on each new variable 
        # to the total dataframe
        
        if test_dat is None:
            return tot_dat
        elif (tot_dat is None) and (test_dat is not None):
            tot_data = test_dat.copy()
            return tot_dat
        
        # Check that the time stamps on the new data and the existing extracted data match.
        # Store the matching indices and non-matching indices separately
        basematchinds = []
        matchinds = []
        nonmatchinds = []
        if len(test_dat) > 1:
            for ii in range(0,len(test_dat)):
                if any(np.logical_and(tot_dat.pt_id == 
                                      test_dat.pt_id.to_numpy().squeeze()[ii],
                                      tot_dat.platform_id == 
                                      test_dat.platform_id.to_numpy().squeeze()[ii])):

                    basematchinds.append(np.where(np.logical_and(tot_dat.pt_id == 
                                                                 test_dat.pt_id.to_numpy().squeeze()[ii],
                                                                 tot_dat.platform_id == 
                                                                 test_dat.platform_id.to_numpy().squeeze()[ii]))[0][0])
                    matchinds.append(ii)
                else:
                    nonmatchinds.append(ii)


            # If all of the indices match, add the data directly to the dataframe
            if len(matchinds) == len(tot_dat):
                tot_dat.loc[basematchinds,varname] = test_dat.loc[matchinds,varname].to_numpy().squeeze()
            else:
                # If any indices do not match...

                # ...first add all the matching indices in the right location
                if len(matchinds) > 0:
                    tot_dat.loc[basematchinds,varname] = test_dat.loc[matchinds,varname].to_numpy().squeeze()

                # ..., and then append on the non-matching indices, 
                # filling with NaNs where necessary
                if len(nonmatchinds) > 0:
                    if all([not(ii == varname) for ii in tot_dat.columns]):
                        print('   ...Variable does not yet exist: ', varname)
                        tot_dat.loc[:,varname] = np.nan*np.ones(len(tot_dat))
                    tot_dat = pd.concat([tot_dat, 
                                         test_dat.iloc[nonmatchinds,:]]).reset_index(drop=True)
                    print('   ...Some mismatched for ' + varname + ': ' + str(len(nonmatchinds)) + ' points') 


        elif len(test_dat) == 1:
            tot_dat.loc[0,varname] = test_dat.loc[0,varname]

        return tot_dat
    
    # Extract out the variable data from the location data
    loc_data_topds = {}
    for varname in location_data.keys():
        loc_data_topds[varname] = {}
        loc_data_topds[varname]['units'] = location_data[varname]['units']
        data_colnames = [ii for ii in location_data[varname]['data'].keys()]
        temp_pd = pd.DataFrame(data = location_data[varname]['data'],
                               columns=data_colnames)
        data_colnames = [varname if ii == 'value' else ii for ii in data_colnames]
        loc_data_topds[varname]['data'] = temp_pd.rename(columns={"value": varname})

    # Get all the variable names
    varnames = [ii for ii in loc_data_topds.keys()]
    
    # Take each variables data, and put it into a dataframe
    # Each variable is split out by whether it is on the regular
    # wave buoy (i.e., at depth = 0), or is a smart mooring sensor
    # (i.e., at a depth != 0)
    if any(loc_data_topds[varnames[0]]['data']['depth'] != 0):
        test_data = loc_data_topds[varnames[0]]['data'].copy()
        
        total_data = test_data[test_data['depth'] == 0].reset_index(drop=True)
        smart_data = test_data[test_data['depth'] != 0].reset_index(drop=True)
    else:
        total_data = loc_data_topds[varnames[0]]['data'].copy()
        smart_data = None
    
    # Create a "point ID", created as a combination of
    # data timestamp and depth
    total_data['pt_id'] = [str(total_data['timestamp'][ii]) 
                           for ii in range(0,len(total_data))]
    
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
        else:
            test_data = alltest_data.copy()
            smarttest_data = None
            
        total_data = append_newvar(total_data, test_data)
        smart_data = append_newvar(smart_data, smarttest_data)
        
        
        
        

            
    
    # Convert data times into actual timestamps
    total_data_time = total_data.timestamp.to_numpy()
    if len(total_data_time) > 1:
        total_data_datetime = [datetime.datetime(1970,1,1) + datetime.timedelta(seconds=int(ii)) 
                               if not(pd.isna(ii)) else pd.NaT 
                               for ii in total_data_time.squeeze()]
    else:
        total_data_datetime = datetime.datetime(1970,1,1) + datetime.timedelta(seconds=int(total_data_time))
    total_data['time'] = total_data_datetime
    
    
    # Drop any data with bad "time" values
    total_data = total_data.dropna(subset='time').reset_index(drop=True)
    
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


def check_for_necessary_variables(df):
    
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


def rename_dataframe_columns(df):
    
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


def get_buoy_qcflags(ds, loc_id):
    
    # Unwrap all the directional angles to
    # prevent large jumps at the 0-360 crossover
    dsnew_qcversion = ds.copy()
    #dsnew_qcversion['sea_surface_wave_from_direction_at_variance_spectral_density_maximum_unwrapped'] = np.unwrap(ds['sea_surface_wave_from_direction_at_variance_spectral_density_maximum'],period=360)
    #dsnew_qcversion['sea_surface_wave_from_direction'] = np.unwrap(ds['sea_surface_wave_from_direction_unwrapped'],period=360)

    # Convert period data into frequency for running the QC tests
    dsnew_qcversion['sea_surface_wave_frequency_at_variance_spectral_density_maximum'] = 1 / dsnew_qcversion['sea_surface_wave_period_at_variance_spectral_density_maximum'].values
    dsnew_qcversion['sea_surface_wave_mean_frequency'] = 1 / dsnew_qcversion['sea_surface_wave_mean_period'].values
    
    # Run QARTOD tests on the data
    qc_limits = bb_qc.load_all_qc_limits(loc_id)
    ds_qc = bb_qc.process_qartod_tests(dsnew_qcversion, dsnew_qcversion.columns,
                                       qc_limits)
    
    return ds_qc


# In[ ]:


def process_newdata(loc_id, rebuild_flag=False, rerun_tests=False):
    
    # Load in existing data, if it exists
    if not(rebuild_flag):
        ds_old = load_existing_netcdf(loc_id)
        if ds_old is not None:
            ds_old = ds_old.sortby('time')
    else:
        ds_old = None
    
    # If there is any existing data, get the last time stamp
    if (ds_old is not None) and not(rebuild_flag):
        lasttime = pd.Timestamp(ds_old['time'].data[-1]).to_pydatetime().strftime('%Y-%m-%dT%H:%M:%SZ')
        print('   Last time stamp of the existing data: ' + lasttime)
    else:
        lasttime = None
        ds_old = None
    
    # Load in the data from the Backyard Buoys data API
    # Note, that for now, nothing is done with the smart mooring data (i.e., "ds_smart")
    ds, ds_smart = get_data_by_location(loc_id, time_start=lasttime)
    if ds is None:
        print('   Return without processing any data')
        return
    
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
            
    
    # Expand the dimensions to include location id
    ds_xr = ds_xr.expand_dims(dim={"location_id":[loc_id]}, axis=0)
    
    
    
    ##################################################
    # Add the new data onto the existing data 
    if ds_old is not None:
        print('   Concat the datasets together')
        ds_all = xr.concat([ds_old, ds_xr.sortby('time')], dim='time').sortby('time')
    else:
        ds_all = ds_xr.sortby('time').copy()
            
    
    return ds_all


# In[ ]:


def rerun_qc_tests(ds_xr, loc_id):
    
    # Make a copy of the xarray dataset
    ds_rerun = ds_xr.to_dataframe().reset_index()
    
    # Rerun the QC flagging on the dataset
    ds_qc = get_buoy_qcflags(ds_rerun, loc_id)
    
    # Update the results in the xarray 
    # dataset for each qc test
    for col in ds_qc.columns:
        ds_xr[col.replace('qartod','qc')].loc[ds_xr['location_id'].data[0],:] = ds_qc.loc[:,col]
        
    return ds_xr


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


def netcdf_add_global_metadata(dataset, loc_id):
    
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

    
    dataset.title = 'backyardbuoys_' + loc_id
    dataset.program = 'Backyard Buoys'
    dataset.program_url = 'https://backyardbuoys.org/'
    dataset.project = 'Backyard Buoys'
    dataset.summary = 'Surface wave and water conditions, as collected as part of the Backyard Buoys program'
    dataset.location_name = meta['location_name']
    dataset.location_id = meta['location_id']
    dataset.ioos_regional_association = meta['ioos_association']
    dataset.ioos_regional_association_url = meta['ioos_url']
    
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
            temp_qcvar[:] = ds[var + '_' + origqc_var]
    
    
    return dataset


# In[ ]:


def write_netcdf(ds, loc_id, datayear):
    
    # If there is no data in the dataframe, do not make a netCDF
    if len(ds) == 0:
        print('No data exists in this file. Do not make a netCDF.')
        return
    
    # Get the base data directory
    basedir = bb.get_datadir()
    
    # Define the folder for the location id,
    # and if the folder does not exist,
    # make the directory
    datadir = os.path.join(basedir, loc_id)
    if not(os.path.exists(datadir)):
        os.mkdir(datadir)
    
    
    # Define the file name
    testfile = 'bb_' + loc_id + '_' + datayear + '.nc'
    
    # Check the number of samples in the file
    nsamps = len(ds['time'])
    
    
    # Open a new netCDF file for writing
    dataset = Dataset(os.path.join(datadir, testfile), 'w', format='NETCDF4')
    print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': ' + testfile + ' open for writing...')
    
    try:
        # Write the global metadata for the netCDF
        dataset = netcdf_add_global_metadata(dataset, loc_id)
        
        # Create the dimensions of the file
        dataset.createDimension('location_id',1)
        dataset.createDimension('time',nsamps)
        
        # Add the variables
        netcdf_add_variables(dataset, loc_id, ds)
        
    except Exception as e:
        # If any errors occur, print out the error message
        print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': something went wrong')
        print(e)
        
    print('   ' + datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') + 
          ': writing ' + testfile + ' complete')
    dataset.close()
    
    return


# # Main Processing Functions

# In[ ]:


def update_data_by_location(loc_id, rebuild_flag=False, rerun_tests=False):
    
    basedir = bb.get_datadir()
    
    # Load in the meta data for all locations
    metadir = os.path.join(basedir, loc_id, 'metadata', loc_id +'_metadata.json')
    if not(os.path.exists(metadir)):
        print(loc_id + ': No metadata exists for this project.')
        print('Try to make the meta data for project: ' + loc_id)
        meta_success = bb_meta.make_project_metadata(loc_id)
        if not(meta_success):
            print('Unable to make the data file for this project')
            return False
    
    # Download any new data
    ds_all = process_newdata(loc_id, rebuild_flag)
    if ds_all is None:
        print('As there is no data, no netCDF is created. End the process, and move on.')
        return False
    
    # If need be, rerun all the QC tests
    if rerun_tests:
        ds_all = rerun_qc_tests(ds_all, loc_id)
    
    # Group all the data by year
    ds_grouped = ds_all.groupby('time.year')
    
    # Loop through each unique year of data, 
    # and write the netcdf file for the
    # location ID for a given year
    print('Years of data to write:', ds_grouped.groups.keys())
    for year in ds_grouped.groups.keys():        
        # Write the netcdf of the file
        write_netcdf(ds_grouped[year].sortby('time'), loc_id, str(year))
    
    return True


# In[ ]:


def update_all_locations(rebuild_flag=False, rerun_tests=False):
    
    # Get a list of the backyard buoys projects
    basedir = bb.get_datadir()
    bb_locs = bb_da.bbapi_get_locations()
    loc_ids = [bb_locs[ii]['loc_id'] for ii in bb_locs]
    loc_active = [True if (bb_locs[ii]['status'] == 'active') 
                  else False for ii in bb_locs]
    
    # If need be, try to create the metadata for each project
    missing_projs = []
    add_projs = []
    for ii in range(0,len(loc_ids)):
        pathdir = os.path.join(basedir, loc_ids[ii], 'metadata', loc_ids[ii]+'_metadata.json')
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


            print(datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') 
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


def check_duplicates(ds):
    
    ds_time = [pd.Timestamp(ii).to_pydatetime() for ii in ds.sortby('time').variables['time'].data]
    if np.any([np.diff(ds_time) == datetime.timedelta(seconds=0)]):
        dupinds = np.where([ii == datetime.timedelta(seconds=0) for ii in np.diff(ds_time)])[0]
        print('Duplicates found to merge... # of duplicates: ' + str(len(dupinds)))
        
        

