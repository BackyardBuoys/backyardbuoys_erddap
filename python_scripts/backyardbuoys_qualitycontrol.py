#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import datetime
import shutil

import numpy as np
import pandas as pd
import requests
import xarray as xr

import os
import sys
import getopt
import gc

import json

import matplotlib
from matplotlib import pyplot as plt

import backyardbuoys_general_functions as bb

import ioos_qc
from ioos_qc import qartod
from ioos_qc.config import Config
from ioos_qc.streams import PandasStream
from ioos_qc.stores import PandasStore


# In[ ]:


def load_all_qc_limits(loc_id):
    
    basedir = bb.get_datadir()
    qc_file = loc_id + '_qartod.json'
    qc_path = os.path.join(basedir, loc_id, 'metadata', qc_file)
    with open(qc_path) as qc_json:
        qc_data = json.load(qc_json)

    # Define the data variable names and the 
    # associated limits sheet names

    bb_vars = ['sea_surface_wave_mean_period',
               'sea_surface_wave_mean_frequency',
               'sea_surface_wave_from_direction',
               'sea_surface_wave_directional_spread',
               'sea_surface_wave_period_at_variance_spectral_density_maximum',
               'sea_surface_wave_frequency_at_variance_spectral_density_maximum',
               'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
               'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
               'sea_surface_wave_significant_height',
               'sea_surface_temperature']

    # Initialize an empty dictionary to hold all the limits
    qc_dict = {}

    # Step through each QC limit sheet name, and parse
    # out the variable limits
    qc_keys = [ii for ii in qc_data['qartod_limits'].keys()]
    for var in bb_vars:
        
        qc_lim_inds = np.where([var in ii for ii in qc_data['qartod_limits']])[0]
        qc_varnames = [qc_keys[ii] for ii in qc_lim_inds]
    
        qc_tests = []
        if any(['gross_range_test' in ii for ii in qc_varnames]):
            qc_tests.append('gross_range_test')
        if any(['spike_test' in ii for ii in qc_varnames]):
            qc_tests.append('spike_test')
        if any(['rate_of_change_test' in ii for ii in qc_varnames]):
            qc_tests.append('rate_of_change_test')
        if any(['flat_line_test' in ii for ii in qc_varnames]):
            qc_tests.append('flat_line_test')

        var_dict = {}
        for qc_test in qc_tests:
            if qc_test == 'gross_range_test':
                var_dict['gross_range_test'] = {
                    'suspect_span': [float(qc_data['qartod_limits'][var+'_gross_range_test_suspect_min']),
                                     float(qc_data['qartod_limits'][var+'_gross_range_test_suspect_max'])],
                    'fail_span': [float(qc_data['qartod_limits'][var+'_gross_range_test_fail_min']),
                                  float(qc_data['qartod_limits'][var+'_gross_range_test_fail_max'])]
                }
            elif qc_test == 'spike_test':
                var_dict['spike_test'] = {
                    'suspect_threshold': float(qc_data['qartod_limits'][var+'_spike_test_suspect']),
                    'fail_threshold': float(qc_data['qartod_limits'][var+'_spike_test_fail'])
                }
            elif qc_test == 'rate_of_change_test':
                var_dict['rate_of_change_test'] = {
                    'threshold': float(qc_data['qartod_limits'][var+'_rate_of_change_test_threshold'])
                }
            elif qc_test == 'flat_line_test':
                var_dict['flat_line_test'] = {
                    'tolerance': float(qc_data['qartod_limits'][var+'_flat_line_test_tolerance']),
                    'suspect_threshold': float(qc_data['qartod_limits'][var+'_flat_line_test_suspect']),
                    'fail_threshold': float(qc_data['qartod_limits'][var+'_flat_line_test_fail'])

                }

        qc_dict[var] = {}
        qc_dict[var]['qartod'] = var_dict
        
    return qc_dict


# In[ ]:


def load_all_qc_limits_excel(loc_id):
    
    datadir = 'C:/Users/APLUser/NANOOS/Backyard_Buoys'
    qc_file = 'backyardbuoys_qartod_limits.xlsx'
    qc_path = os.path.join(datadir, qc_file)

    # Define the data variable names and the 
    # associated limits sheet names
    qc_vars = ['mean_wave_period',
               'mean_wave_frequency',
               'mean_wave_direction',
               'mean_wave_directional_spread',
               'peak_wave_period',
               'peak_wave_frequency',
               'peak_wave_direction',
               'peak_wave_directional_spread',
               'surface_wave_significant_height',
               'sea_surface_temperature']

    bb_vars = ['sea_surface_wave_mean_period',
               'sea_surface_wave_mean_frequency',
               'sea_surface_wave_from_direction',
               'sea_surface_wave_directional_spread',
               'sea_surface_wave_period_at_variance_spectral_density_maximum',
               'sea_surface_wave_frequency_at_variance_spectral_density_maximum',
               'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
               'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
               'sea_surface_wave_significant_height',
               'sea_surface_temperature']

    # Initialize an empty dictionary to hold all the limits
    qc_dict = {}

    # Step through each QC limit sheet name, and parse
    # out the variable limits
    for ii in range(0,len(qc_vars)):
    
        # Load in the right sheet
        qc_df = pd.read_excel(qc_path, sheet_name=qc_vars[ii])


        # Find the data for a given entry
        loc_id_ind = 0
        if any(qc_df.loc_id == loc_id):
            loc_id_ind = np.where(qc_df.loc_id == loc_id)[0]
        else:
            print('Get default limits for ' + bb_vars[ii])
            loc_id_ind = np.where(qc_df.loc_id == 'default')[0]

        qc_df = qc_df.iloc[loc_id_ind].drop(columns='loc_id').reset_index(drop=True)
        qc_df_cols = qc_df.columns

        qc_tests = []
        if any(['gross_range_test' in ii for ii in qc_df_cols]):
            qc_tests.append('gross_range_test')
        if any(['spike_test' in ii for ii in qc_df_cols]):
            qc_tests.append('spike_test')
        if any(['rate_of_change_test' in ii for ii in qc_df_cols]):
            qc_tests.append('rate_of_change_test')
        if any(['flat_line_test' in ii for ii in qc_df_cols]):
            qc_tests.append('flat_line_test')

        var_dict = {}
        for qc_test in qc_tests:
            if qc_test == 'gross_range_test':
                var_dict['gross_range_test'] = {
                    'suspect_span': [qc_df['gross_range_test_suspect_min'].values[0],
                                     qc_df['gross_range_test_suspect_max'].values[0]],
                    'fail_span': [qc_df['gross_range_test_fail_min'].values[0],
                                  qc_df['gross_range_test_fail_max'].values[0]]
                }
            elif qc_test == 'spike_test':
                var_dict['spike_test'] = {
                    'suspect_threshold': qc_df['spike_test_suspect'].values[0],
                    'fail_threshold': qc_df['spike_test_fail'].values[0]
                }
            elif qc_test == 'rate_of_change_test':
                var_dict['rate_of_change_test'] = {
                    'threshold': qc_df['rate_of_change_test_threshold'].values[0]
                }
            elif qc_test == 'flat_line_test':
                var_dict['flat_line_test'] = {
                    'tolerance': qc_df['flat_line_tolerance'].values[0],
                    'suspect_threshold': qc_df['flat_line_test_suspect'].values[0],
                    'fail_threshold': qc_df['flat_line_test_fail'].values[0]

                }

        qc_dict[bb_vars[ii]] = {}
        qc_dict[bb_vars[ii]]['qartod'] = var_dict
        
    return qc_dict


# In[ ]:


def load_sensor_qartod_config(sensor):
    
    config = {}
    
    if 'period' in sensor.lower():
        config = {
            sensor: {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 25],
                        "fail_span": [-1, 50]},
                    "spike_test": {
                        "suspect_threshold": 5,
                        "fail_threshold": 10},
                    "rate_of_change_test": {
                        "threshold": 5/(15*60)}  
                }
            }
        }
        if 'variance_spectral_density_maximum' in sensor.lower():
            config[sensor]['qartod']['flat_line_test'] = {"tolerance": 0.05,
                                                          "suspect_threshold": 36*(60*60),
                                                          "fail_threshold": 72*(30*60)}
        else:
            config[sensor]['qartod']['flat_line_test'] = {"tolerance": 0.05,
                                                          "suspect_threshold": 6*(60*60),
                                                          "fail_threshold": 12*(60*60)}
            
    elif 'frequency' in sensor.lower():
        config = {
            sensor: {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0.05, 0.25],
                        "fail_span": [0.03, 1.0]},
                    "spike_test": {
                        "suspect_threshold": 0.05,
                        "fail_threshold": 0.1},
                    "rate_of_change_test": {
                        "threshold": 0.05/(30*60)}
                }
            }
        }
        if 'variance_spectral_density_maximum' in sensor.lower():
            config[sensor]['qartod']['flat_line_test'] = {"tolerance": (2.5-0.1)/256,
                                                          "suspect_threshold": 36*(60*60),
                                                          "fail_threshold": 72*(60*60)}
        else:
            config[sensor]['qartod']['flat_line_test'] = {"tolerance": (2.5-0.1)/256,
                                                          "suspect_threshold": 6*(60*60),
                                                          "fail_threshold": 12*(60*60)}
            
    elif ('direction' in sensor.lower()) and not('directional_spread' in sensor.lower()):
        config = {
            sensor: {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 360],
                        "fail_span": [-180, 540]},
                    "spike_test": {
                        "suspect_threshold": 90,
                        "fail_threshold": 180},
                    "rate_of_change_test": {
                        "threshold": 180/(15*60)},
                    "flat_line_test": {
                        "tolerance": 0.01,
                        "suspect_threshold": 4*(30*60),
                        "fail_threshold": 12*(30*60)}
                }
            }
        }
    elif 'directional_spread' in sensor.lower():
        config = {
            sensor: {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 90],
                        "fail_span": [-90, 180]},
                    "spike_test": {
                        "suspect_threshold": 10,
                        "fail_threshold": 20},
                    "rate_of_change_test": {
                        "threshold": 180/(15*60)},
                    "flat_line_test": {
                        "tolerance": 0.1,
                        "suspect_threshold": 4*(30*60),
                        "fail_threshold": 12*(30*60)}
                }
            }
        }
    elif 'wave_significant_height' in sensor.lower():
        config = {
            sensor: {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 15],
                        "fail_span": [-0.1, 25]},
                    "spike_test": {
                        "suspect_threshold": 3,
                        "fail_threshold": 5},
                    "rate_of_change_test": {
                        "threshold": 3/(15*60)},
                    "flat_line_test": {
                        "tolerance": 0.01,
                        "suspect_threshold": 4*(30*60),
                        "fail_threshold": 12*(30*60)}
                }
            }
        }
    elif ('temperature' in sensor.lower()) or ('temp' in sensor.lower()):
        config = {
            sensor: {
                "qartod": {
                    "gross_range_test": {
                        "suspect_span": [0, 30],
                        "fail_span": [-5, 50]},
                    "spike_test": {
                        "suspect_threshold": 1.0,
                        "fail_threshold": 2.0},
                    "rate_of_change_test": {
                        "threshold": 2/(15*60)},
                    "flat_line_test": {
                        "tolerance": 0.01,
                        "suspect_threshold": 3*(60*60),
                        "fail_threshold": 6*(60*60)}
                }
            }
        }
        
    # Add lat/lon tests
    
        
    return config


# In[ ]:


def run_qartod_tests(var_df, sensor, config):
    
    # Ensure that the variable is a pandas dataframe
    if not(isinstance(var_df,pd.DataFrame)):
        var_df = pd.DataFrame(data=var_df,columns=['time',sensor])
        
        
    # Load in the qartod configuration settings
    c = Config(config)

    # Setup the stream
    ps = PandasStream(var_df.loc[:,['time',sensor]], time='time')
    # ps = PandasStream(df, time='time', z='z', lat='lat', lon='lon', geom='geom')
    # Pass the run method the config to use
    results = ps.run(c)
    
    # Store the results in another DataFrame
    store = PandasStore(
        results
    )

    # Write only the test results to the store
    results_store = store.save(write_data=False, write_axes=False)
    

    #
    aggr_flags = qartod.qartod_compare(results_store.to_numpy().T)
    results_store[sensor + '_qartod_rollup_qc'] = aggr_flags
    
    
    return results_store


# In[1]:


def process_qartod_tests(ds, sensor_names, qc_limits):
    
    qartod_valid_sensors = ['sea_surface_wave_significant_height','sea_surface_temperature', 
                            'sea_surface_wave_period_at_variance_spectral_density_maximum', 
                            'sea_surface_wave_from_direction_at_variance_spectral_density_maximum', 
                            'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
                            'sea_surface_wave_mean_period', 
                            'sea_surface_wave_from_direction', 'sea_surface_wave_directional_spread',
                            'sea_surface_wave_frequency_at_variance_spectral_density_maximum', 
                            'sea_surface_wave_mean_frequency']
    qartod_df = []
    NT = ds['time'].size
    
    time_qartod = np.array([datetime.datetime.strptime(ii,'%Y-%m-%d %H:%M:%S') for ii in 
                            [ii.strftime('%Y-%m-%d %H:%M:%S') for ii in 
                             [pd.Timestamp(ii) for ii in ds['time'].to_numpy()]]])
    
    for sensor in sensor_names:
        if (sensor in qartod_valid_sensors):
            var_df = ds.loc[:,['time',sensor]]
            temp_sens = var_df[sensor].to_numpy().flatten()
            
            # Ensure that any "bad" data is given as a NaN
            # (and not np.inf or any other invalid data type)
            temp_sens[var_df[sensor].isna()] = np.nan
            var_df[sensor] = temp_sens
            var_df['time'] = time_qartod
            
            if not((all(var_df[sensor].isna())) or (all(var_df[sensor] == -555))):

                # Perform the qartod tests for the specified sensor
                sensor_qc_limits = {sensor: qc_limits[sensor]}
                temp_qartod_df = run_qartod_tests(var_df, sensor, sensor_qc_limits)        
                
                
                if 'from_direction' in sensor:
                    # Make a copy of the dataframe, and unwrap the directional data in it
                    var_df2 = var_df.copy()
                    var_df2.loc[:,sensor] = np.unwrap(var_df.loc[:,sensor], period=360)
                    
                    # Rerun the QC tests on the unwrapped data
                    temp_qartod_df2 = run_qartod_tests(var_df2, sensor, sensor_qc_limits)
                    
                    # Update the spike/rate-of-change/flat-line tests with the 
                    # results from the unwrapped data tests
                    temp_qartod_df[sensor + '_qartod_rate_of_change_test'] = temp_qartod_df2[sensor + '_qartod_rate_of_change_test']
                    temp_qartod_df[sensor + '_qartod_spike_test'] = temp_qartod_df2[sensor + '_qartod_spike_test']
                    temp_qartod_df[sensor + '_qartod_flat_line_test'] = temp_qartod_df2[sensor + '_qartod_flat_line_test']

                    # Create new aggregated flags, based upon the 
                    # new qc test results
                    aggr_flags = qartod.qartod_compare(temp_qartod_df.drop(columns=[sensor + '_qartod_rollup_qc']).to_numpy().T)
                    temp_qartod_df[sensor + '_qartod_rollup_qc'] = aggr_flags

                temp_results = concat_test_results_into_string(temp_qartod_df)

                temp_qartod_compiled = pd.DataFrame(data=list(zip(np.ndarray.flatten(temp_qartod_df[sensor+'_qartod_rollup_qc'].to_numpy()),
                                                                  temp_results)),
                                                    columns=[sensor+'_qc_agg',
                                                             sensor+'_qc_tests'])
            
                temp_qartod_final = temp_qartod_df.copy()
                for col in temp_qartod_compiled.columns:
                    temp_qartod_final[col] = temp_qartod_compiled[col]
                

            else:
                # If all the data is bad, apply "fail" flags everywhere, and assign 9999 for the tests, indicating
                # that no tests were performed
                temp_qartod_final = pd.DataFrame(data=list(zip(4*np.ones(NT).astype(int),
                                                               9999*np.ones(NT).astype(int))),
                                                 columns=[sensor+'_qc_agg',
                                                          sensor+'_qc_tests']) 
                qc_tests = load_sensor_qartod_config(sensor)
                for test in qc_tests[sensor]['qartod'].keys():
                    temp_qartod_final[sensor+'_qartod_'+test] = 9*np.ones(NT).astype(int)
                
        
            
            # Append on the newly created qartod flags to the qartod dataframe
            if isinstance(qartod_df,list):
                qartod_df = temp_qartod_final
            else:
                qartod_df = pd.concat([qartod_df, temp_qartod_final],axis=1)
                
    
    # Copy the flags for frequency into the flags for period
    for col in qartod_df.columns:
        if 'period' in col:
            # Create the matching frequency columns name as the
            # current period column name
            freq_col = col.replace('period','frequency')            
            
            # Replace the values in the period column with
            # those from the frequency column
            qartod_df[col] = qartod_df[freq_col]
            
            # Drop the frequency columns
            qartod_df = qartod_df.drop(columns=freq_col)
            
            
    # Drop the rollup qc flag
    for col in qartod_df.columns:
        if 'qartod_rollup_qc' in col:
            # Drop the rollup qc flag column
            qartod_df = qartod_df.drop(columns=col)
        if '_qc_tests' in col:
            # Drop the rollup qc flag column
            qartod_df = qartod_df.drop(columns=col)

            
    return qartod_df


# In[ ]:


def concat_test_results_into_string(temp_qartod_df):
    
    # Initialize an empty list to store the concatenated qartod results
    qartod_results = np.zeros(len(temp_qartod_df)).astype(int)
    
    # Extract out the relevant column names
    qar_cols = temp_qartod_df.columns
    # Only concatenate the result if it is not the rollup result
    qar_cols = [ii for ii in qar_cols if 'qartod_rollup_qc' not in ii]
    
    # Loop through each column, and append on the digit of the current
    # flag
    # Example, if there are 3 flags (flag 1 = 1, flag 2 = 4, flag 3 = 1),
    # then the final result should be 141.
    # To get there, we use 100*flag1 + 10*flag2 + 1*flag3
    # which is the same as (10^2)*flag1 + (10^1)*flag2 + (10^0)*flag3
    # The maximum exponent factor is equal to (# of flags - 1)
    fact = len(qar_cols) - 1
    for column in qar_cols:
        qartod_results = qartod_results + (temp_qartod_df.loc[:,column].astype(float) * (10**fact)).astype(int)
        fact = fact - 1
     
    # Change the result from an array into a list
    qartod_results = [ii for ii in qartod_results]

    return qartod_results


# In[ ]:


def add_qc_attrs(ds, df_qc):
    
    ds_qc = xr.Dataset()
    ds_qc["time"] = xr.DataArray(
            np.array(ds['time']), dims="time"
        )
    for x in df_qc.columns:
        ds_qc[x] = xr.DataArray(
            np.array(df_qc[x]), dims='time'
        )

    for x in ds_qc.keys():
        if '_qc_agg' in x:
            ds_qc[x].attrs["long_name"] = x
            ds_qc[x].attrs['description'] = 'QARTOD Aggregate flag for ' + x
            ds_qc[x].attrs['flag_values'] = '1, 2, 3, 4, 9'
            ds_qc[x].attrs['flag_meanings'] = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            ds_qc[x].attrs["_FillValue"] = -555
        elif '_qc_tests' in x:
            ds_qc[x].attrs["long_name"] = x
            ds_qc[x].attrs['description'] = 'QARTOD Tests performed for ' + x
            ds_qc[x].attrs['flag_values'] = '1, 2, 3, 4, 9'
            ds_qc[x].attrs['flag_meanings'] = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            ds_qc[x].attrs['comment'] = '4-character string with results of individual QARTOD tests. 1: Gross Range Test, 2: Spike Test, 3: Flat-line Test, 4: Rate-of-change Test'
            ds_qc[x].attrs["_FillValue"] = -555
        elif '_qartod_gross_range_test' in x:
            ds_qc[x].attrs["long_name"] = x
            ds_qc[x].attrs['description'] = 'QARTOD Gross Range Test performed for ' + x[:x.find('_qartod_gross_range_test')]
            ds_qc[x].attrs['flag_values'] = '1, 2, 3, 4, 9'
            ds_qc[x].attrs['flag_meanings'] = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            ds_qc[x].attrs["_FillValue"] = -555
        elif '_qartod_spike_test' in x:
            ds_qc[x].attrs["long_name"] = x
            ds_qc[x].attrs['description'] = 'QARTOD Spike Test performed for ' + x[:x.find('_qartod_spike_test')]
            ds_qc[x].attrs['flag_values'] = '1, 2, 3, 4, 9'
            ds_qc[x].attrs['flag_meanings'] = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            ds_qc[x].attrs["_FillValue"] = -555
        elif '_qartod_flat_line_test' in x:
            ds_qc[x].attrs["long_name"] = x
            ds_qc[x].attrs['description'] = 'QARTOD Flat-line Test performed for ' + x[:x.find('_qartod_flat_line_test')]
            ds_qc[x].attrs['flag_values'] = '1, 2, 3, 4, 9'
            ds_qc[x].attrs['flag_meanings'] = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            ds_qc[x].attrs["_FillValue"] = -555
        elif '_qartod_rate_of_change_test' in x:
            ds_qc[x].attrs["long_name"] = x
            ds_qc[x].attrs['description'] = 'QARTOD Rate-of-Change Test performed for ' + x[:x.find('_qartod_rate_of_change_test')]
            ds_qc[x].attrs['flag_values'] = '1, 2, 3, 4, 9'
            ds_qc[x].attrs['flag_meanings'] = 'PASS NOT_EVALUATED SUSPECT FAIL MISSING'
            ds_qc[x].attrs["_FillValue"] = -555
            
            
    return ds_qc

