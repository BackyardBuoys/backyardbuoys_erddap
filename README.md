# BackyardBuoys ERDDAP Server

![License](https://img.shields.io/badge/license-Check%20LICENSE%20file-blue)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)

## Overview

The BackyardBuoys ERDDAP project manages the data pipeline and ERDDAP server configuration for the Backyard Buoys program. This system enables Indigenous and coastal communities to gather and use wave data collected from Sofar Spotter buoys, enhancing their blue economy and hazard protection efforts.

### Key Features

- **Automated Data Processing**: Fetches data from the Backyard Buoys API and processes it into CF-compliant NetCDF files
- **Quality Control**: Implements IOOS QARTOD (Quality Assurance/Quality Control of Real-Time Oceanographic Data) tests
- **Metadata Management**: Pulls metadata from Google Sheets and generates standardized JSON files
- **ERDDAP Integration**: Automatically generates and updates ERDDAP dataset XML configurations
- **Multi-location Support**: Manages data from multiple buoy deployment locations

## Project Structure

```
backyardbuoys_erddap/
├── erddap_files/
│   ├── base_datasets.xml       # Base ERDDAP configuration
│   ├── datasets.xml            # Active ERDDAP datasets configuration
│   └── dataset_template.xml    # Template for new dataset entries
├── python_scripts/
│   ├── backyardbuoys_main.py              # Main entry point and CLI
│   ├── backyardbuoys_dataaccess.py        # API data access functions
│   ├── backyardbuoys_processdata.py       # Data processing and NetCDF generation
│   ├── backyardbuoys_qualitycontrol.py    # QARTOD QC implementation
│   ├── backyardbuoys_build_metadata.py    # Metadata compilation from Google Sheets
│   ├── backyardbuoys_generate_xml.py      # ERDDAP XML generation
│   ├── backyardbuoys_general_functions.py # Utility functions
│   └── info_jsons/
│       ├── bbapi_info.json     # Backyard Buoys API endpoints
│       └── google_info.json    # Google Sheets configuration
├── README.md
└── LICENSE
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Required Python packages:
  - numpy
  - pandas
  - xarray
  - netCDF4
  - requests
  - ioos_qc
  - google-auth
  - google-auth-oauthlib
  - google-auth-httplib2
  - google-api-python-client

### Setup

1. Clone the repository:
```bash
git clone https://github.com/BackyardBuoys/backyardbuoys_erddap.git
cd backyardbuoys_erddap
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure API access:
   - Set up Google Sheets API credentials
   - Configure Backyard Buoys API endpoint information in `info_jsons/bbapi_info.json`

4. Set up directory paths:
   - Update paths in `backyardbuoys_general_functions.py` for your system
   - Default paths:
     - Data directory: `/data/tomcat/dataset_files/`
     - Scripts directory: `/home/stravis/backyardbuoys_files/`

## Usage

### Command Line Interface

The main script `backyardbuoys_main.py` provides a CLI for managing the data pipeline:

```bash
python backyardbuoys_main.py -p <process> -l <location> [-r <rebuild>] [-q <qctests>]
```

#### Options

- `-p, --process`: Specify the process to run
  - `addData`: Add new data or update data for a location
  - `addDataset`: Add a new dataset to ERDDAP XML
  - `addMetadata`: Add or update metadata JSON for a location

- `-l, --location`: Specify location ID (or "all" for all locations)

- `-r, --rebuild`: Rebuild datasets from scratch (true/false)
  - Only valid with `addData` process

- `-q, --qctests`: Rerun quality control tests (true/false)
  - Only valid with `addData` process

- `-h, --help`: Display help information

### Examples

#### Add New Data for a Specific Location
```bash
python backyardbuoys_main.py -p addData -l quileute_south
```

#### Rebuild All Data with QC Tests
```bash
python backyardbuoys_main.py -p addData -l all -r true -q true
```

#### Update Metadata for a Location
```bash
python backyardbuoys_main.py -p addMetadata -l quileute_south
```

#### Add Dataset to ERDDAP
```bash
python backyardbuoys_main.py -p addDataset -l quileute_south
```

## Data Processing Workflow

### 1. Metadata Compilation
- Fetches metadata from Google Sheets
- Fetches QARTOD QC limits from Google Sheets
- Generates location-specific metadata JSON files
- Stores metadata in `{location_id}/metadata/` directory

### 2. Data Acquisition
- Queries Backyard Buoys API for location data
- Retrieves wave parameters and sea surface temperature
- Handles incremental updates based on existing data

### 3. Quality Control
- Applies IOOS QARTOD tests:
  - Gross Range Test
  - Spike Test
  - Rate of Change Test
  - Flat Line Test
- Generates aggregate quality flags
- Handles directional data wrapping for accurate QC

### 4. Data Processing
- Converts API data to pandas DataFrames
- Renames variables to CF standard names
- Merges data with existing NetCDF files
- Groups data by year for file management

### 5. NetCDF Generation
- Creates CF-1.10 compliant NetCDF files
- Includes comprehensive metadata following IOOS standards
- Embeds QC flags as ancillary variables
- Organizes files by location and year: `bb_{location_id}_{year}.nc`

### 6. ERDDAP Configuration
- Generates dataset XML entries from template
- Updates master `datasets.xml` file
- Maintains alphabetical ordering of datasets
- Archives previous configurations

## Data Standards

### Metadata Standards
- **CF-1.10**: Climate and Forecast Metadata Conventions
- **ACDD-1.3**: Attribute Convention for Data Discovery
- **IOOS-1.2**: IOOS Metadata Profile

### Quality Control
- **QARTOD**: IOOS Quality Assurance/Quality Control of Real-Time Oceanographic Data
- Flag values: 1 (PASS), 2 (NOT_EVALUATED), 3 (SUSPECT), 4 (FAIL), 9 (MISSING)

### Variables
- Wave significant height (`sea_surface_wave_significant_height`)
- Mean wave period (`sea_surface_wave_mean_period`)
- Mean wave direction (`sea_surface_wave_from_direction`)
- Peak wave period (`sea_surface_wave_period_at_variance_spectral_density_maximum`)
- Peak wave direction (`sea_surface_wave_from_direction_at_variance_spectral_density_maximum`)
- Sea surface temperature (`sea_surface_temperature`)
- Directional spread parameters
- QC flags for all variables

## Configuration Files

### API Endpoints (`bbapi_info.json`)
```json
{
  "get_locations": "https://data.backyardbuoys.org/get_locations",
  "get_location_data": "https://data.backyardbuoys.org/get_location_data",
  "get_platform_data": "https://data.backyardbuoys.org/get_platform_data"
}
```

### Google Sheets Configuration (`google_info.json`)
Should contain:
- Metadata sheet ID
- QARTOD limits sheet ID

## Architecture

### Module Dependencies
```
backyardbuoys_main.py
    ├── backyardbuoys_processdata.py
    │   ├── backyardbuoys_dataaccess.py
    │   ├── backyardbuoys_qualitycontrol.py
    │   ├── backyardbuoys_build_metadata.py
    │   └── backyardbuoys_general_functions.py
    ├── backyardbuoys_build_metadata.py
    │   ├── backyardbuoys_dataaccess.py
    │   └── backyardbuoys_general_functions.py
    └── backyardbuoys_generate_xml.py
        ├── backyardbuoys_dataaccess.py
        ├── backyardbuoys_processdata.py
        └── backyardbuoys_general_functions.py
```

## Error Handling

The system includes comprehensive error handling for:
- Missing metadata files
- API connection failures
- Invalid location IDs
- Data processing errors
- NetCDF file creation issues

## Contributing

When contributing to this project:
1. Maintain CF and IOOS compliance
2. Document all new functions with docstrings
3. Add appropriate error handling
4. Test with sample data before production deployment
5. Update this README with any new features

## Data Citation

When using Backyard Buoys data, cite as specified in the dataset metadata:

```
Institution. Year. dataset_id. Backyard Buoys. dataset_url
```

Example:
```
Quileute Indian Tribe. 2024. backyardbuoys_quileute_south. 
Backyard Buoys. https://backyardbuoys.org/erddap/backyardbuoys_quileute_south
```

## Resources

- [Backyard Buoys Website](https://backyardbuoys.org/)
- [IOOS QARTOD](https://ioos.noaa.gov/project/qartod/)
- [ERDDAP Documentation](https://coastwatch.pfeg.noaa.gov/erddap/index.html)
- [CF Conventions](http://cfconventions.org/)
- [IOOS Metadata Profile](https://ioos.github.io/ioos-metadata/)

## Support

For questions or issues:
- Email: setht1@uw.edu
- Website: https://backyardbuoys.org/

## License

See LICENSE file for details.

## Acknowledgments

This project is supported by:
- NSF Convergence Accelerator
- IOOS (Integrated Ocean Observing System)
- Participating Indigenous and coastal communities
