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

import matplotlib
from matplotlib import pyplot as plt

import ioos_qc
from ioos_qc import qartod
from ioos_qc.config import Config
from ioos_qc.streams import PandasStream
from ioos_qc.stores import PandasStore


# # Backyard Buoys API

# In[ ]:


def bbapi_get_locations():
    
    api_url = "https://data.backyardbuoys.org/get_locations"
    response = requests.get(url=api_url)
    lines = np.array(response.json())
    
    loc_data = {}
    for line in lines:
        loc_data[line['loc_id']] = {}
        for item in line:
            loc_data[line['loc_id']][item] = line[item]
    
    return loc_data


# In[ ]:


def bbapi_get_location_data(loc_id, vars_to_get = 'ALL', time_start=None, time_end=None):
    
    api_url = "https://data.backyardbuoys.org/get_location_data"
    params = {
            "loc_id": loc_id,
            "var_id": vars_to_get
        }
    if time_start is not None:
        params['time_start'] = time_start
    if time_end is not None:
        params['time_end'] = time_end
        
    response = requests.get(url=api_url, params=params)

    lines = response.json()
    
    if len(lines['variables']) == 0:
        return None
    
    location_data = {}
    for line in lines['variables']:
        var_name = line['var_id']
        location_data[var_name] = {}
        location_data[var_name]['units'] = line['units']
        
        extract_data = {}
        for item in line['data'][0]:
            extract_data[item] = []

        for entry in line['data']:
            for item in entry:
                extract_data[item].append(entry[item])
            
        location_data[var_name]['data'] = extract_data
        
    return location_data


# In[ ]:


def bbapi_get_platform_data(platform_id):
    
    # Platform ID example: 'SPOT-30880C'
    api_url = "https://data.backyardbuoys.org/get_platform_data"
    params = {
        "platform_id": platform_id,
        "var_id": 'ALL'
    }

    response = requests.get(url=api_url, params=params)
    lines = response.json()

    return lines


# # Sofar API

# In[ ]:


def get_buoydata_sofarapi(basedir, spotterID, timeStart, apikey_file):
    
    # Download data from the Sofar API
    headers = {}
    with open(os.path.join(basedir,apikey_file)) as f:
        headers["token"] = f.read().strip()
        
    params = {
        "spotterId": spotterID,
        "limit": "500",
        "startDate": timeStart,
        "includeWaves": "true",
        "includeWindData": "false",
        "includeSurfaceTempData": "true",
        "includeFrequencyData": "false",
        "includeDirectionalMoments": "false",
        "includePartitionData": "false",
    }
    
    api_url = "https://api.sofarocean.com/api/wave-data"
    response = requests.get(url=api_url, headers=headers, params=params)
    
    if response.status_code == 400:
        print('API Data request - status code 400: Client error - Bad request')
        print('Return with no data')
        ds = None
        return ds
    elif response.status_code == 401:
        print('API Data request - status code 401: Client error - Unauthorized')
        print('Return with no data')
        ds = None
        return ds
    elif response.status_code == 404:
        print('API Data request - status code 404: Client error - Not found')
        print('Return with no data')
        ds = None
        return ds
    elif response.status_code == 500:
        print('API Data request - status code 500: Server error')
        print('Return with no data')
        ds = None
        return ds

    lines = np.array(response.json()["data"]["waves"])
    if len(lines) == 0:
        ds = None
        return ds


# In[ ]:


def smartmooring(basedir, apikey_file, spotterID, startDate, endDate):
    
    # Download data from the Sofar API
    headers = {}
    with open(os.path.join(basedir,apikey_file)) as f:
        headers["token"] = f.read().strip()
        
    # Set the smart mooring parameters needed for download
    smart_params = {}
    smart_params['spotterId'] = spotterID
    smart_params['startDate'] = startDate
    smart_params['endDate'] = endDate

    smart_url = "https://api.sofarocean.com/api/sensor-data"
    smart_response = requests.get(url=smart_url, headers=headers, params=smart_params)
    
    if smart_response.status_code == 400:
        print('API Data request - status code 400: Client error - Bad request')
        print('Return with no data')
        ds = None
        return ds
    elif smart_response.status_code == 401:
        print('API Data request - status code 401: Client error - Unauthorized')
        print('Return with no data')
        ds = None
        return ds
    elif smart_response.status_code == 404:
        print('API Data request - status code 404: Client error - Not found')
        print('Return with no data')
        ds = None
        return ds
    elif smart_response.status_code == 500:
        print('API Data request - status code 500: Server error')
        print('Return with no data')
        ds = None
        return ds
    
    
    lines = smart_response.json()["data"]

    # Initialize empty lists
    mptime1 = []
    mpval1 = []
    mptime2 = []
    mpval2 = []
    mttime1 = []
    mtval1 = []
    mttime2 = []
    mtval2 = []
    
    # Step through each line and extract the data
    for line in lines:
        
        # Add pressure from sensor position 1
        if (
            line["data_type_name"] == "rbrcoda3_meanpressure_21bits"
            and line["sensorPosition"] == 1
        ):
            mptime.append(line["timestamp"])
            mpval.append(line["value"])
        
        # Add pressure from sensor position 2
        if (
            line["data_type_name"] == "rbrcoda3_meanpressure_21bits"
            and line["sensorPosition"] == 2
        ):
            mptime.append(line["timestamp"])
            mpval.append(line["value"])
        
        # Add temperature from sensor position 1
        if (
            line["data_type_name"] == "sofar_temperature_12bits"
            and line["sensorPosition"] == 1
        ):
            mttime1.append(line["timestamp"])
            mtval1.append(line["value"])
        
        # Add temperature from sensor position 2    
        if (
            line["data_type_name"] == "sofar_temperature_12bits"
            and line["sensorPosition"] == 2
        ):
            mttime2.append(line["timestamp"])
            mtval2.append(line["value"])
         
    
    
    # Create an xarray dataset for pressure at sensor position 1
    meanpres1 = xr.Dataset()
    meanpres1["time"] = xr.DataArray([np.datetime64(x) for x in mptime1], dims="time")
    meanpres1["sea_water_pressure_pos1"] = xr.DataArray(
        np.array(mpval1) * 0.00001, dims="time"
    )
    meanpres1["sea_water_pressure_pos1"].attrs["units"] = "dbar"
    
    
    # Create an xarray dataset for pressure at sensor position 2
    meanpres2 = xr.Dataset()
    meanpres2["time"] = xr.DataArray([np.datetime64(x) for x in mptime1], dims="time")
    meanpres2["sea_water_pressure_pos2"] = xr.DataArray(
        np.array(mpval1) * 0.00001, dims="time"
    )
    meanpres2["sea_water_pressure_pos2"].attrs["units"] = "dbar"

    
    # Create an xarray dataset for temperature at sensor position 1
    meantemp1 = xr.Dataset()
    meantemp1["time"] = xr.DataArray([np.datetime64(x) for x in mttime1], dims="time")
    meantemp1["sea_water_temperature_pos1"] = xr.DataArray(
        np.array(mtval1), dims="time"
    )
    meantemp1["sea_water_temperature_pos1"].attrs["units"] = "degree_C"
    
    
    # Create an xarray dataset for temperature at sensor position 1
    meantemp2 = xr.Dataset()
    meantemp2["time"] = xr.DataArray([np.datetime64(x) for x in mttime2], dims="time")
    meantemp2["sea_water_temperature_pos2"] = xr.DataArray(
        np.array(mtval2), dims="time"
    )
    meantemp2["sea_water_temperature_pos2"].attrs["units"] = "degree_C"

    return xr.merge([meanpres1, meanpres2, 
                     meantemp1, meantemp2])


# In[ ]:




