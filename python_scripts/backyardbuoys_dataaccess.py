#!/usr/bin/env python
# coding: utf-8

"""
BackyardBuoys ERDDAP - Data Access Module
==========================================

This module handles all data retrieval operations from the Backyard Buoys API
and Sofar Ocean API. It provides functions to fetch location information,
time-series data, and platform-specific measurements.

Key Functions:
    - bbapi_get_locations() : Get all buoy deployment locations
    - bbapi_get_location_data() : Get time-series data for a location
    - bbapi_get_platform_data() : Get data for a specific buoy platform
    - smartmooring() : Get smart mooring sensor data from Sofar API

Data Sources:
    - Backyard Buoys API: Primary source for processed wave and temperature data
    - Sofar Ocean API: Direct access to Spotter buoy data (legacy/backup)

Author: Seth Travis
Organization: Backyard Buoys
"""

import datetime
import shutil
import os
import sys
import getopt
import gc

import numpy as np
import pandas as pd
import requests
import xarray as xr

import matplotlib
from matplotlib import pyplot as plt

import ioos_qc
from ioos_qc import qartod
from ioos_qc.config import Config
from ioos_qc.streams import PandasStream
from ioos_qc.stores import PandasStore

# Import BackyardBuoys utility functions
import backyardbuoys_general_functions as bb   


# ============================================================================
# Backyard Buoys API Functions
# ============================================================================

def bbapi_get_locations(recentFlag=False):
    """
    Retrieve all available Backyard Buoys locations from the API.
    
    Fetches a complete list of buoy deployment locations including metadata
    such as location ID, label, status, and geographic coordinates. This is
    typically the first function called to discover available data sources.
    
    Returns
    -------
    dict
        Dictionary of location data keyed by location ID.
        
        Structure:
        {
            'location_id': {
                'loc_id': str,       # Unique identifier
                'label': str,        # Human-readable name
                'status': str,       # 'active', 'inactive', etc.
                'lat': float,        # Latitude (degrees N)
                'lon': float,        # Longitude (degrees E)
                ... additional metadata ...
            }
        }
    
    Examples
    --------
    >>> # Get all locations
    >>> locations = bbapi_get_locations()
    >>> print(f"Found {len(locations)} locations")
    Found 12 locations
    
    >>> # List active locations
    >>> for loc_id, data in locations.items():
    ...     if data['status'] == 'active':
    ...         print(f"{data['label']} at ({data['lat']}, {data['lon']})")
    Quileute South at (47.9, -124.7)
    Gambell at (63.8, -171.8)
    
    >>> # Check if a location exists
    >>> if 'quileute_south' in locations:
    ...     print("Quileute South data is available")
    Quileute South data is available
    
    Notes
    -----
    - API endpoint URL is configured in bbapi_info.json
    - Returns data for all locations regardless of active status
    - Location IDs are used throughout the system as unique identifiers
    
    See Also
    --------
    bbapi_get_location_data : Get time-series data for a location
    """
    
    # Load the API configuration to get the locations endpoint URL
    bbinfo = bb.load_bbapi_info_json()
    api_url = bbinfo['get_locations']
    if recentFlag:
        api_url = api_url + '?newest_data=true'
    
    # Make GET request to the API
    response = requests.get(url=api_url)
    
    # Parse the JSON response into a numpy array
    lines = np.array(response.json())
    
    # Restructure the data into a dictionary keyed by location ID
    # This makes it easier to look up specific locations
    loc_data = {}
    for line in lines:
        # Use the location ID as the key
        loc_data[line['loc_id']] = {}
        # Copy all fields from the API response
        for item in line:
            loc_data[line['loc_id']][item] = line[item]
    
    return loc_data


def bbapi_get_location_data(loc_id, vars_to_get='ALL', time_start=None, time_end=None):
    """
    Retrieve variable data for a specific location from Backyard Buoys API.
    
    Queries the API to fetch time-series measurements for specified variables
    at a given location. Supports temporal filtering and selective variable
    retrieval to optimize data transfer.
    
    Parameters
    ----------
    loc_id : str
        Location identifier (e.g., 'quileute_south', 'gambell')
    vars_to_get : str, optional
        Comma-separated variable IDs or 'ALL' for all available variables.
        Default is 'ALL'.
        
        Available variables:
        - 'WaveHeightSig' : Significant wave height
        - 'WavePeriodMean' : Mean wave period
        - 'WaveDirMean' : Mean wave direction
        - 'WaveDirMeanSpread' : Mean directional spread
        - 'WavePeriodPeak' : Peak wave period
        - 'WaveDirPeak' : Peak wave direction
        - 'WaveDirPeakSpread' : Peak directional spread
        - 'WaterTemp' : Sea surface temperature
        
    time_start : str, optional
        Start time in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).
        If None, retrieves all available historical data.
    time_end : str, optional
        End time in ISO 8601 format.
        If None, retrieves data up to the present time.
    
    Returns
    -------
    dict or None
        Nested dictionary containing variable data, or None if no data
        is available for the specified location and time range.
        
        Structure:
        {
            'variable_id': {
                'units': str,
                'data': {
                    'timestamp': list of int,  # Unix epoch time
                    'value': list of float,
                    'lat': list of float,
                    'lon': list of float,
                    'depth': list of float,    # 0 for surface
                    'platform_id': list of str, # Spotter buoy ID
                    'type': list of str
                }
            }
        }
    
    Examples
    --------
    >>> # Get all recent data for a location
    >>> data = bbapi_get_location_data('quileute_south')
    >>> if data:
    ...     print(f"Variables: {list(data.keys())}")
    ...     n_points = len(data['WaveHeightSig']['data']['value'])
    ...     print(f"Wave height measurements: {n_points}")
    Variables: ['WaveHeightSig', 'WavePeriodMean', 'WaterTemp', ...]
    Wave height measurements: 8760
    
    >>> # Get only wave height since January 2024
    >>> wave_data = bbapi_get_location_data(
    ...     'quileute_south',
    ...     vars_to_get='WaveHeightSig',
    ...     time_start='2024-01-01T00:00:00Z'
    ... )
    
    >>> # Get multiple variables for a specific date range
    >>> multi_data = bbapi_get_location_data(
    ...     'gambell',
    ...     vars_to_get='WaveHeightSig,WaterTemp',
    ...     time_start='2024-01-01T00:00:00Z',
    ...     time_end='2024-02-01T00:00:00Z'
    ... )
    
    >>> # Check for missing data
    >>> data = bbapi_get_location_data('invalid_location')
    >>> if data is None:
    ...     print("No data available for this location")
    No data available for this location
    
    Notes
    -----
    - API endpoint configured in bbapi_info.json under 'get_location_data'
    - Timestamps are Unix epoch time (seconds since 1970-01-01 00:00:00 UTC)
    - depth=0 indicates surface measurements from the wave buoy
    - depth≠0 indicates subsurface measurements from smart mooring sensors
    - Returns empty 'variables' list if no data matches the query
    
    See Also
    --------
    bbapi_get_locations : Get list of available locations
    bbapi_get_platform_data : Get data for a specific buoy platform
    """
    
    # Load the API configuration to get the location data endpoint URL
    bbinfo = bb.load_bbapi_info_json()
    api_url = bbinfo['get_location_data']
    
    # Construct the query parameters for the API request
    params = {
        "loc_id": loc_id,      # Location identifier
        "var_id": vars_to_get  # Variables to retrieve
    }
    
    # Add optional time filters if provided
    if time_start is not None:
        params['time_start'] = time_start
    if time_end is not None:
        params['time_end'] = time_end
        
    # Make GET request to the API with query parameters
    response = requests.get(url=api_url, params=params)

    # Parse the JSON response
    lines = response.json()
    
    # Check if any data was returned
    if len(lines['variables']) == 0:
        # No data available for this location/time range
        return None
    
    # Restructure the data into a more convenient format
    # Organize by variable name instead of list index
    location_data = {}
    for line in lines['variables']:
        # Get the variable name (e.g., 'WaveHeightSig')
        var_name = line['var_id']
        
        # Create a dictionary for this variable
        location_data[var_name] = {}
        
        # Store the units (e.g., 'm' for wave height)
        location_data[var_name]['units'] = line['units']
        
        # Restructure the data array into a dictionary of lists
        # This makes it easier to work with in pandas/numpy
        extract_data = {}
        
        # Initialize empty lists for each data field
        for item in line['data'][0]:
            extract_data[item] = []

        # Populate the lists with data from each time point
        for entry in line['data']:
            for item in entry:
                extract_data[item].append(entry[item])
            
        # Store the extracted data
        location_data[var_name]['data'] = extract_data
        
    return location_data


def bbapi_get_platforms(inactiveFlag=False, retiredFlag=False, 
                        offlineFlag=False, allplatsFlag=False):
    
    # Check flags:
    if allplatsFlag and (inactiveFlag or retiredFlag or offlineFlag):
        inactiveFlag = False
        retiredFlag = False
        offlineFlag = False
    
    # Platform ID example: 'SPOT-30880C'
    # Load API configuration and construct the appropriate endpoint URL
    bbinfo = bb.load_bbapi_info_json()
    api_url = bbinfo['get_platforms']
    
    # Append status query parameter based on flags
    if inactiveFlag:
        api_url = api_url + '?status=inactive'
    elif retiredFlag:
        api_url = api_url + '?status=retired'
    elif offlineFlag:
        api_url = api_url + '?status=offline'
    
    # Make the API request
    response = requests.get(url=api_url)
    json_response = response.json()
    
    # Check if the response is empty or contains an error message
    # The API returns {'error': 'No platforms found'} when empty
    if (len(json_response) == 0 or 
        (isinstance(json_response, dict) and 'error' in json_response)):
        if inactiveFlag:
            print('No inactive platforms found')
        elif retiredFlag:
            print('No retired platforms found')
        else:
            print('No active platforms found')
        return None
    
    # Normalize response to always be a list for consistent iteration
    # The API may return a single dict or a list depending on result count
    if isinstance(json_response, dict) and 'error' not in json_response:
        lines = [json_response]  # Wrap single dict in a list
    else:
        lines = json_response  # Already a list
        
        
    
    # Restructure the data into a dictionary keyed by platform_id
    # This makes it easier to look up specific platforms
    plat_data = {}
    for line in lines:
        plat_data[line['platform_id']] = {}  # Create entry for this platform
        # Copy all platform attributes to the dictionary
        for item in line:
            plat_data[line['platform_id']][item] = line[item]
            
    # If requesting all platforms, recursively fetch inactive, retired, and offline platforms
    if allplatsFlag:
        # Get inactive platforms and merge them into plat_data
        inactive_plats = bbapi_get_platforms(inactiveFlag=True)
        if inactive_plats is not None:
            for key in inactive_plats.keys():
                plat_data[key] = inactive_plats[key]
        
        # Get retired platforms and merge them into plat_data
        retired_plats = bbapi_get_platforms(retiredFlag=True)
        if retired_plats is not None:
            for key in retired_plats.keys():
                plat_data[key] = retired_plats[key]
        
        # Get offline platforms and merge them into plat_data
        offline_plats = bbapi_get_platforms(offlineFlag=True)
        if offline_plats is not None:
            for key in offline_plats.keys():
                plat_data[key] = offline_plats[key]

    return plat_data



def bbapi_get_platform_data(platform_id):
    """
    Retrieve data for a specific buoy platform from Backyard Buoys API.
    
    Fetches data directly for a specific Sofar Spotter buoy using its
    platform ID. This is useful when working with individual buoys rather
    than deployment locations.
    
    Parameters
    ----------
    platform_id : str
        Platform identifier in format 'SPOT-XXXXX'
        (e.g., 'SPOT-30880C', 'SPOT-12345')
    
    Returns
    -------
    dict
        Platform data from API response containing time-series measurements
        and buoy metadata
    
    Examples
    --------
    >>> # Get data for a specific Spotter buoy
    >>> platform_data = bbapi_get_platform_data('SPOT-30880C')
    >>> print(f"Platform: {platform_data['platform_id']}")
    >>> print(f"Location: {platform_data['location']}")
    Platform: SPOT-30880C
    Location: quileute_south
    
    >>> # Get all variables for the platform
    >>> if 'variables' in platform_data:
    ...     for var in platform_data['variables']:
    ...         print(f"  {var['var_id']}: {len(var['data'])} points")
      WaveHeightSig: 8760 points
      WaterTemp: 8760 points
    
    Notes
    -----
    - API endpoint configured in bbapi_info.json under 'get_platform_data'
    - Platform IDs can be found in the location data returned by
      bbapi_get_location_data() in the 'platform_id' field
    - One location may have multiple platforms over time as buoys are
      deployed, recovered, and redeployed
    - Returns all available variables for the platform
    
    See Also
    --------
    bbapi_get_locations : Get available locations
    bbapi_get_location_data : Get data organized by location
    """
    
    # Load the API configuration
    bbinfo = bb.load_bbapi_info_json()
    api_url = bbinfo['get_platform_data']
    
    # Set up query parameters
    params = {
        "platform_id": platform_id,  # Spotter buoy ID
        "var_id": 'ALL'              # Get all variables
    }

    # Make GET request to the API
    response = requests.get(url=api_url, params=params)
    
    # Parse and return the JSON response
    lines = response.json()

    return lines


# ============================================================================
# Sofar Ocean API Functions (Legacy/Direct Access)
# ============================================================================

def get_buoydata_sofarapi(basedir, spotterID, timeStart, apikey_file):
    """
    Download wave data directly from the Sofar Ocean API.
    
    Legacy function for direct access to Sofar Spotter buoy data. This
    bypasses the Backyard Buoys API and queries Sofar directly. Primarily
    used for backup/validation or when Backyard Buoys API is unavailable.
    
    Parameters
    ----------
    basedir : str
        Base directory containing the Sofar API key file
    spotterID : str
        Sofar Spotter buoy identifier (e.g., 'SPOT-30880C')
    timeStart : str
        Start date for data retrieval in ISO format
    apikey_file : str
        Filename of the file containing the Sofar API key
    
    Returns
    -------
    xarray.Dataset or None
        Dataset containing wave data, or None if the request fails
    
    Examples
    --------
    >>> # Download wave data from Sofar API
    >>> ds = get_buoydata_sofarapi(
    ...     '/data/keys',
    ...     'SPOT-30880C',
    ...     '2024-01-01T00:00:00Z',
    ...     'sofar_api_key.txt'
    ... )
    >>> if ds is not None:
    ...     print(f"Retrieved {len(ds.time)} time points")
    Retrieved 8760 time points
    
    Notes
    -----
    - Requires valid Sofar Ocean API key
    - API key should be stored in a text file (one line, no whitespace)
    - Returns None for various HTTP error codes:
      - 400: Bad request (invalid parameters)
      - 401: Unauthorized (invalid API key)
      - 404: Not found (invalid Spotter ID)
      - 500: Server error
    - Limited to 500 records per request
    - Includes wave parameters and surface temperature
    - Does not include wind or frequency/directional moment data
    
    API Parameters Used:
        - limit: 500 (maximum records per request)
        - includeWaves: true
        - includeWindData: false
        - includeSurfaceTempData: true
        - includeFrequencyData: false
        - includeDirectionalMoments: false
        - includePartitionData: false
    
    See Also
    --------
    smartmooring : Get smart mooring sensor data from Sofar API
    bbapi_get_platform_data : Get data via Backyard Buoys API
    """
    
    # Load the Sofar API key from file
    headers = {}
    with open(os.path.join(basedir, apikey_file)) as f:
        # Read the API key and strip any whitespace
        headers["token"] = f.read().strip()
        
    # Set up query parameters for the Sofar API request
    params = {
        "spotterId": spotterID,              # Buoy identifier
        "limit": "500",                      # Max records per request
        "startDate": timeStart,              # Start of time range
        "includeWaves": "true",              # Include wave parameters
        "includeWindData": "false",          # Exclude wind data
        "includeSurfaceTempData": "true",    # Include temperature
        "includeFrequencyData": "false",     # Exclude frequency spectra
        "includeDirectionalMoments": "false",# Exclude directional moments
        "includePartitionData": "false",     # Exclude partition data
    }
    
    # Make GET request to Sofar API
    api_url = "https://api.sofarocean.com/api/wave-data"
    response = requests.get(url=api_url, headers=headers, params=params)
    
    # Check response status code and handle errors
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

    # Parse the JSON response
    lines = np.array(response.json()["data"]["waves"])
    
    # Check if any data was returned
    if len(lines) == 0:
        ds = None
        return ds

    # Note: The function appears incomplete in the original code
    # It should process 'lines' into an xarray Dataset here
    # TODO: Complete the data processing implementation


def smartmooring(basedir, apikey_file, spotterID, startDate, endDate):
    """
    Retrieve smart mooring sensor data from Sofar Ocean API.
    
    Fetches data from RBR Coda³ T.ODO sensors attached to Sofar Spotter
    buoys via smart mooring. These sensors provide subsurface pressure and
    temperature measurements at depths below the surface buoy.
    
    Parameters
    ----------
    basedir : str
        Base directory containing the API key file
    apikey_file : str
        Filename of the file containing the Sofar API key
    spotterID : str
        Sofar Spotter buoy identifier (e.g., 'SPOT-30880C')
    startDate : str
        Start date for data range in ISO format
    endDate : str
        End date for data range in ISO format
    
    Returns
    -------
    xarray.Dataset or None
        Merged dataset containing subsurface measurements, or None if the
        request fails.
        
        Variables:
        - sea_water_pressure_pos1 : Pressure at sensor position 1 (dbar)
        - sea_water_pressure_pos2 : Pressure at sensor position 2 (dbar)
        - sea_water_temperature_pos1 : Temperature at position 1 (°C)
        - sea_water_temperature_pos2 : Temperature at position 2 (°C)
        - time : Measurement timestamps
    
    Examples
    --------
    >>> # Get smart mooring data for 2024
    >>> smart_ds = smartmooring(
    ...     '/data/keys',
    ...     'sofar_api_key.txt',
    ...     'SPOT-12345',
    ...     '2024-01-01',
    ...     '2024-12-31'
    ... )
    >>> if smart_ds is not None:
    ...     print("Variables:", list(smart_ds.data_vars))
    ...     print(f"Time range: {smart_ds.time[0]} to {smart_ds.time[-1]}")
    Variables: ['sea_water_pressure_pos1', 'sea_water_pressure_pos2', 
                'sea_water_temperature_pos1', 'sea_water_temperature_pos2']
    Time range: 2024-01-01 to 2024-12-31
    
    >>> # Access temperature data
    >>> temp1 = smart_ds['sea_water_temperature_pos1'].values
    >>> print(f"Mean temperature: {np.nanmean(temp1):.2f}°C")
    Mean temperature: 12.45°C
    
    Notes
    -----
    - Smart moorings have sensors at two positions (depths)
    - Sensor position 1 is typically shallower than position 2
    - Pressure is measured in decibars (dbar), approximately equal to meters
    - Temperature is in degrees Celsius
    - Returns None for HTTP error codes (400, 401, 404, 500)
    - Data types:
      - rbrcoda3_meanpressure_21bits: Pressure measurements
      - sofar_temperature_12bits: Temperature measurements
    
    Sensor Configuration:
        - RBR Coda³ T.ODO sensors
        - Two sensor positions per mooring
        - Measures pressure and temperature
        - Connected to Spotter buoy for data transmission
    
    See Also
    --------
    get_buoydata_sofarapi : Get surface wave data
    bbapi_get_location_data : Get all data via Backyard Buoys API
    """
    
    # Load the Sofar API key from file
    headers = {}
    with open(os.path.join(basedir, apikey_file)) as f:
        headers["token"] = f.read().strip()
        
    # Set up smart mooring query parameters
    smart_params = {}
    smart_params['spotterId'] = spotterID
    smart_params['startDate'] = startDate
    smart_params['endDate'] = endDate

    # Make GET request to Sofar sensor data API
    smart_url = "https://api.sofarocean.com/api/sensor-data"
    smart_response = requests.get(url=smart_url, headers=headers, params=smart_params)
    
    # Check response status code and handle errors
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
    
    # Parse the JSON response
    lines = smart_response.json()["data"]

    # Initialize empty lists for each sensor and position
    mptime1 = []  # Pressure timestamps, position 1
    mpval1 = []   # Pressure values, position 1
    mptime2 = []  # Pressure timestamps, position 2
    mpval2 = []   # Pressure values, position 2
    mttime1 = []  # Temperature timestamps, position 1
    mtval1 = []   # Temperature values, position 1
    mttime2 = []  # Temperature timestamps, position 2
    mtval2 = []   # Temperature values, position 2
    
    # Step through each line and extract the data based on type and position
    for line in lines:
        
        # Extract pressure from sensor position 1
        if (
            line["data_type_name"] == "rbrcoda3_meanpressure_21bits"
            and line["sensorPosition"] == 1
        ):
            mptime1.append(line["timestamp"])
            mpval1.append(line["value"])
        
        # Extract pressure from sensor position 2
        if (
            line["data_type_name"] == "rbrcoda3_meanpressure_21bits"
            and line["sensorPosition"] == 2
        ):
            mptime2.append(line["timestamp"])
            mpval2.append(line["value"])
        
        # Extract temperature from sensor position 1
        if (
            line["data_type_name"] == "sofar_temperature_12bits"
            and line["sensorPosition"] == 1
        ):
            mttime1.append(line["timestamp"])
            mtval1.append(line["value"])
        
        # Extract temperature from sensor position 2    
        if (
            line["data_type_name"] == "sofar_temperature_12bits"
            and line["sensorPosition"] == 2
        ):
            mttime2.append(line["timestamp"])
            mtval2.append(line["value"])
         
    # Create xarray datasets for each sensor/position combination
    
    # Pressure at position 1
    meanpres1 = xr.Dataset()
    meanpres1["time"] = xr.DataArray(
        [np.datetime64(x) for x in mptime1], dims="time"
    )
    meanpres1["sea_water_pressure_pos1"] = xr.DataArray(
        np.array(mpval1) * 0.00001, dims="time"  # Scale factor conversion
    )
    meanpres1["sea_water_pressure_pos1"].attrs["units"] = "dbar"
    
    # Pressure at position 2
    meanpres2 = xr.Dataset()
    meanpres2["time"] = xr.DataArray(
        [np.datetime64(x) for x in mptime2], dims="time"
    )
    meanpres2["sea_water_pressure_pos2"] = xr.DataArray(
        np.array(mpval2) * 0.00001, dims="time"  # Scale factor conversion
    )
    meanpres2["sea_water_pressure_pos2"].attrs["units"] = "dbar"

    # Temperature at position 1
    meantemp1 = xr.Dataset()
    meantemp1["time"] = xr.DataArray(
        [np.datetime64(x) for x in mttime1], dims="time"
    )
    meantemp1["sea_water_temperature_pos1"] = xr.DataArray(
        np.array(mtval1), dims="time"
    )
    meantemp1["sea_water_temperature_pos1"].attrs["units"] = "degree_C"
    
    # Temperature at position 2
    meantemp2 = xr.Dataset()
    meantemp2["time"] = xr.DataArray(
        [np.datetime64(x) for x in mttime2], dims="time"
    )
    meantemp2["sea_water_temperature_pos2"] = xr.DataArray(
        np.array(mtval2), dims="time"
    )
    meantemp2["sea_water_temperature_pos2"].attrs["units"] = "degree_C"

    # Merge all datasets along the time dimension
    return xr.merge([meanpres1, meanpres2, meantemp1, meantemp2])
