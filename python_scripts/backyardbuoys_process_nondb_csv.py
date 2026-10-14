#!/usr/bin/env python
# coding: utf-8

"""
Process non-database CSV data into Backyard Buoys NetCDF files.

This script ingests a CSV file from the directory specified by "non-db_data"
in bb_dirs.json, reruns QARTOD QC checks using the same logic as the main
processing pipeline, and writes month-partitioned NetCDF files.
"""

import argparse
import datetime
import json
import os
import sys

import numpy as np
import pandas as pd

import backyardbuoys_processdata as bb_process


DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def _load_directory_info():
    basedir = os.path.dirname(__file__)
    dirs_path = os.path.join(basedir, 'bb_dirs.json')
    with open(dirs_path, 'r') as dir_json:
        return json.load(dir_json)


def _resolve_csv_path(non_db_dir, location_id, csv_file=None):
    if csv_file is not None:
        if os.path.isabs(csv_file):
            if not os.path.exists(csv_file):
                raise FileNotFoundError(f'CSV file not found: {csv_file}')
            return csv_file

        candidate = os.path.join(non_db_dir, csv_file)
        if not os.path.exists(candidate):
            raise FileNotFoundError(
                f'CSV file "{csv_file}" not found under non-db_data directory: {non_db_dir}'
            )
        return candidate

    location_hyphen = location_id.replace('_', '-')
    csv_candidates = []
    for root, _, files in os.walk(non_db_dir):
        for file_name in files:
            if not file_name.lower().endswith('.csv'):
                continue
            file_lc = file_name.lower()
            if ((location_id.lower() in file_lc) or
                    (location_hyphen.lower() in file_lc)):
                csv_candidates.append(os.path.join(root, file_name))

    if len(csv_candidates) == 0:
        raise FileNotFoundError(
            f'No CSV matching location "{location_id}" found under: {non_db_dir}'
        )

    csv_candidates = sorted(csv_candidates, key=os.path.getmtime, reverse=True)
    if len(csv_candidates) > 1:
        print('Multiple CSV files matched location ID. Using the most recent file:')
    print('   ' + csv_candidates[0])
    return csv_candidates[0]


def _to_standard_columns(df):
    rename_map = {
        'lat': 'latitude',
        'lon': 'longitude',
        'WaveHeightSig': 'sea_surface_wave_significant_height',
        'WavePeriodMean': 'sea_surface_wave_mean_period',
        'WaveDirMean': 'sea_surface_wave_from_direction',
        'WaveDirMeanSpread': 'sea_surface_wave_directional_spread',
        'WavePeriodPeak': 'sea_surface_wave_period_at_variance_spectral_density_maximum',
        'WaveDirPeak': 'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
        'WaveDirPeakSpread': 'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
        'WaterTemp': 'sea_water_temperature',
        'sea_surface_temperature': 'sea_water_temperature'
    }
    return df.rename(columns=rename_map)


def _prepare_dataframe(raw_df, location_id):
    df = raw_df.copy()
    df = _to_standard_columns(df)

    if ('time' in df.columns) and (len(df) > 0):
        first_time = str(df.iloc[0]['time']).strip().upper()
        if first_time == 'UTC':
            df = df.iloc[1:].reset_index(drop=True)

    if 'time' not in df.columns:
        if 'timestamp' in df.columns:
            df['time'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce', utc=True)
        else:
            raise ValueError('CSV must contain either "time" or "timestamp" column.')
    else:
        df['time'] = pd.to_datetime(df['time'], format='ISO8601', errors='coerce', utc=True)

    if isinstance(df['time'].dtype, pd.DatetimeTZDtype):
        df['time'] = df['time'].dt.tz_convert(None)

    df = df.dropna(subset=['time']).reset_index(drop=True)
    if len(df) == 0:
        raise ValueError('No valid timestamp rows remain after parsing the CSV.')

    if 'platform_id' not in df.columns:
        if 'buoy_id' in df.columns:
            df['platform_id'] = df['buoy_id'].astype(str)
        else:
            df['platform_id'] = location_id[:11]
    else:
        df['platform_id'] = df['platform_id'].astype(str)

    required_vars = [
        'latitude',
        'longitude',
        'sea_surface_wave_significant_height',
        'sea_surface_wave_mean_period',
        'sea_surface_wave_from_direction',
        'sea_surface_wave_directional_spread',
        'sea_surface_wave_period_at_variance_spectral_density_maximum',
        'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
        'sea_surface_wave_directional_spread_at_variance_spectral_density_maximum',
        'sea_water_temperature'
    ]

    for col in required_vars:
        if col not in df.columns:
            df[col] = np.nan

    for col in required_vars:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if df['latitude'].isna().all() or df['longitude'].isna().all():
        raise ValueError('CSV must provide at least some valid latitude and longitude values.')

    keep_cols = ['time', 'platform_id'] + required_vars
    df = df[keep_cols].sort_values('time').reset_index(drop=True)
    return df


def _build_xarray_with_qc(df, location_id):
    ds_qc = bb_process.get_buoy_qcflags(df.copy(), location_id)
    ds_df = pd.concat([df, ds_qc], axis=1)

    ds_xr = ds_df.set_index('time').to_xarray()
    for varname in list(ds_xr.keys()):
        if '_qartod_' in varname:
            ds_xr = ds_xr.rename({varname: varname.replace('_qartod_', '_qc_')})

    ds_xr = ds_xr.assign(buoy_id=('time', ds_df['platform_id']))
    ds_xr = ds_xr.expand_dims(dim={'location_id': [location_id]}, axis=0).sortby('time')
    ds_xr = bb_process.check_duplicates(ds_xr)
    return ds_xr


def process_csv_to_netcdf(location_id, csv_file=None):
    if bb_process.get_location_metadata(location_id) is None:
        raise FileNotFoundError(
            f'Metadata JSON not found for "{location_id}". Run metadata generation first.'
        )

    dir_info = _load_directory_info()
    if 'non-db_data' not in dir_info:
        raise KeyError('Missing "non-db_data" in bb_dirs.json')

    non_db_dir = dir_info['non-db_data']
    csv_path = _resolve_csv_path(non_db_dir, location_id, csv_file=csv_file)
    print('Reading CSV file: ' + csv_path)

    raw_df = pd.read_csv(csv_path)
    df = _prepare_dataframe(raw_df, location_id)
    ds_xr = _build_xarray_with_qc(df, location_id)

    ds_grouped = ds_xr.groupby('time.year')
    print('Years of data to write:', list(ds_grouped.groups.keys()))
    for year in ds_grouped.groups.keys():
        ds_subgrouped = ds_grouped[year].groupby('time.month')
        print('Months of data to write:', list(ds_subgrouped.groups.keys()))
        for month in ds_subgrouped.groups.keys():
            bb_process.write_netcdf(ds_subgrouped[month].sortby('time'),
                                    location_id, year, month, nondb_flag=True)

    print('Processing complete at ' + datetime.datetime.now().strftime(DATETIME_FORMAT))


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Ingest non-db CSV data for a location, run QC, and write NetCDF files.'
    )
    parser.add_argument(
        '-l', '--location',
        required=True,
        help='Location ID (e.g., tomales_bay)'
    )
    parser.add_argument(
        '-f', '--file',
        default=None,
        help=('CSV file path. If relative, it is resolved under "non-db_data" from bb_dirs.json. '
              'If omitted, the script auto-selects the most recent matching CSV for the location.')
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    try:
        process_csv_to_netcdf(args.location, args.file)
    except Exception as exc:
        print('Error processing CSV to NetCDF:')
        print(str(exc))
        sys.exit(1)
