# End Use Load Profiles for the U.S. Building Stock
This collection of datasets describes the energy consumption of the U.S. residential and commercial building stock. The data are broken down first by building type (single family home, office, restaurant, etc.), then by end-use (heating, cooling, lighting, etc.) at a 15-minute time interval. For details on how the datasets were created and validated, please visit the [project website](https://www.nrel.gov/buildings/end-use-load-profiles.html).

## Directory Structure
```
nrel-pds-building-stock                                                 # top-level directory inside the NREL oedi-data-lake bucket
├── end-use-load-profiles-for-us-building-stock                         # directory for all End Use Load Profiles and (future) End Use Savings Shapes
│   ├── README.md                                                       # the file you are reading right now
│   └── 2021                                                            # year the dataset was published
│       └── comstock_amy2018_release_1                                  # name of the dataset
│           ├── README.md                                               # description of dataset and updates since last published version
│           ├── citation.txt                                            # citation for this dataset
│           ├── data_dictionary.tsv                                     # column names, units, enumerations, and descriptions
│           ├── enumeration_dictionary.tsv                              # mapping between enumeration name and description
│           ├── upgrade_dictionary.tsv                                  # mapping between upgrade identifier and upgrade name and description
│           ├── building_energy_models                                  # OpenStudio models used to generate timeseries data
│           │   ├── <building_id>-up<upgrade_id>.osm.gz                 # by building_id and upgrade_id
│           ├── correction_factors                                      # correction factors, only found for residential datasets
│           │    ├── correction_factors_2018.csv                        # factors may be used to adjust certain end-uses post-simulation
│           ├── geographic_information                                  # geographic shapefiles used for mapping this dataset
│           │    ├── map_of_pumas_in_census_region_1_northeast.geojson  # map of U.S. Census Public Use Microdata Area in Census Region 1 Northeast
│           │    ├── map_of_pumas_in_census_region_2_midwest.geojson    # map of U.S. Census Public Use Microdata Area in Census Region 2 Midwest
│           │    ├── map_of_pumas_in_census_region_3_south.geojson      # map of U.S. Census Public Use Microdata Area in Census Region 3 South
│           │    ├── map_of_pumas_in_census_region_4_west.geojson       # map of U.S. Census Public Use Microdata Area in Census Region 4 West
│           │    ├── map_of_us_states.geojson                           # map of U.S. States
│           │    └── spatial_tract_lookup_table.csv                     # mapping between census tract identifiers and other geographies
│           ├── metadata                                                # building characteristics and annual energy consumption for each building
│           │   └── metadata.parquet                                    # building characteristics and annual energy consumption for each building
│           ├── timeseries_aggregates                                   # sum of all profiles in a given geography by building type and end use
│           │   ├── by_ashrae_iecc_climate_zone_2004                    # by ASHRAE climate zone
│           │   │   ├── <geography_id>-<building_type>.csv              # aggregate timeseries energy consumption by end use and fuel type
│           │   ├── by_building_america_climate_zone                    # by DOE Building America Climate Zone
│           │   ├── by_county                                           # by U.S. County
│           │   ├── by_iso_rto_region                                   # by Electric System ISO
│           │   └── by_puma                                             # by U.S. Census Public Use Microdata Area
|           |── timeseries_aggregates_metadata                          # metadata information about the timeseries aggregates
│           │   └── metadata.tsv                                        # building characteristics and annual energy consumption for each building
│           ├── timeseries_individual_buildings                         # individual building timeseries data, partitioned several ways for faster queries
│           │   ├── by_county                                           # by U.S. County
│           │   │    ├── upgrade=<upgrade_id>                           # numerical identifier of upgrade (0 = baseline building stock)
│           │   │    │   └── county=<county_id>                         # gisjoin identifiers for counties and PUMAs, postal abbreviation for states
│           │   │    │       ├── <building_id>-<upgrade_id>.parquet     # individual building timeseries data
│           │   ├── by_puma_midwest                                     # by U.S. Census Public Use Microdata Area in Census Region 2 Midwest
│           │   ├── by_puma_northeast                                   # by U.S. Census Public Use Microdata Area in Census Region 1 Northeast
│           │   ├── by_puma_south                                       # by U.S. Census Public Use Microdata Area in Census Region 3 South
│           │   ├── by_puma_west                                        # by U.S. Census Public Use Microdata Area in Census Region 4 West
│           │   ├── by_state                                            # by State
│           ├── weather                                                 # weather data used to run the building energy models to create datasets
│           │   ├── amy<year>                                           # weather data for a specific year (from NOAA ISD, NSRDB, and MesoWest)
│           │   │   ├── <location_id>_<year>.csv                        # by location, county gisjoin identifier
│           │   ├── tmy3                                                # weather data used for typical weather run
│           │   │   ├── <location_id>_tmy3.csv                          # by location, county gisjoin identifier

```

## Citation

Please use the citation found in `citation.txt` for each dataset when referencing this work.

## Dataset Naming
```
         <dataset type>_<weather data>_<year of publication>_release_<release number>
 example:    comstock        amy2018            2021         release_1
```
  - dataset type
    - resstock = residential buildings stock
    - comstock = commercial building stock
  - weather data
    - amy2018 = actual meteorological year 2018 (2018 weather data from NOAA ISD, NSRDB, and MesoWest)
    - tmy3 = typical weather from 1991-2005 (see [this publication](https://www.nrel.gov/docs/fy08osti/43156.pdf) for details)
  - year of publication
    - 2021 = dataset was published in 2021
    - 2022 = dataset was published in 2022
    - ...
  - release
    - release_1 = first release of the dataset during the year of publication
    - release_2 = second release of the dataset during the year of publication
    - ...

## Metadata
These are the building characteristics (age, area, HVAC system type, etc.) for each of the buildings
energy models run to create the timeseries data. Descriptions of these characteristics are
included in `data_dictionary.tsv`, `enumeration_dictionary.tsv`, and `upgrade_dictionary.tsv`.

## Aggregated Timeseries
Aggregate end-use load profiles by building type and geography that can be opened
and analyzed in Excel, python, or other common data analysis tools.
Each file includes the summed energy consumption for all buildings of the
specified type in the geography of interest by 15-minute timestep.

In addition to the timeseries data, each file includes a header
which lists the number of building energy models used to create the aggregate,
as well as the list of `building_ids`, which can be cross-referenced with
the `metadata.csv` file to extract building characteristics for all
buildings whose data are included in the sums.

### Aggregate timeseries are available by each of the following geographies:
-  U.S. States
-  ASHRAE Climate Zones
-  DOE Building America Climate Zones
-  Electric System ISOs
-  U.S. Census Public Use Microdata Area
-  U.S. Counties
    - **WARNING** in sparsely-populated counties, the number of models included in
    the aggregates may be very low (single digits), causing the aggregate load profiles
    to be unrealistic. When using county-level aggregates, we highly recommend that you
    review these numbers (included in the file header) and the load profiles before using them.

## Individual Building Timeseries
The raw individual building timeseries data.  **This is a large number of individual files!**
These data are partitioned (organized) in several different ways for comstock.nrel.gov and resstock.nrel.gov data viewers

Partitions:

-  U.S. States
-  PUMAS in Census Region 1 Northeast
-  PUMAS in Census Region 2 Midwest
-  PUMAS in Census Region 3 South
-  PUMAS in Census Region 4 West

## Building Energy Models
These are the building energy models, in [OpenStudio](https://www.openstudio.net/) format, that were run to create
the dataset. These building energy models use the [EnergyPlus](https://energyplus.net/) building simulation engine.

## Geographic Information
Information on various geographies used in the dataset provided for convenience. Includes
map files showing the shapes of the geographies (states, PUMAs) used for partitioning
and a lookup table mapping between census tracts and various other geographies.

## Weather
These files show the key weather data used as an input to run the building energy models to create the dataset.
These data are provided in `CSV` format (instead of the EnergyPlus `EPW` format) for easier analysis.

The **AMY** (actual meteorological year) files contain measured weather data from a specific year.
See [this publication](forthcoming_EULP_final_report) for details on how the AMY files were created.
The datasets created using AMY files are appropriate for applications where it
is important that the impacts of a weather event (for example, a regional heat wave) are realistically
synchronized across locations.

The **TMY3** (typical meteorological year) files contain typical weather from 1991-2005.
See [this publication](https://www.nrel.gov/docs/fy08osti/43156.pdf) for details on how the TMY3
files were created. The datasets created using TMY3 files are appropriate for applications
where a more "average" load profile is desired. **Note:** the weather data in the TMY3 files is NOT
synchonized between locations. One region could be experiencing a heat wave while another has mild temperatures.

# File formats

The tables below illustrate the format of key files in the dataset.
**Note** that TSV file format was selected to allow commas in names and descriptions.

## data_dictionary.tsv

Describes the column names found in the metadata and timeseries data files.
All building characteristics start with `in.` and all timeseries outputs start with `out.`
Enumerations are separated with the `|` character.

| field_location  | field_name                                  | data_type | units | field_description | allowable_enumerations  |
|----------       |--------------                               |------     |---    |---                |---                      |
| metadata        | building_id                                 | int       |       |                   |                         |
| metadata        | job_id                                      | int       |       |                   |                         |
| metadata        | in.completed_status                         | bool      |       |                   |Success\|Fail            |
| metadata        | in.code_when_built                          | string    |       |                   |90.1-2004\|90.1-2007     |
| ...             |                                             |           |       |                   |                         |
| timeseries      | Time                                        | time      |       |                   |                         |
| timeseries      | TimeDST                                     | time      |       |                   |                         |
| timeseries      | TimeUTC                                     | time      |       |                   |                         |
| timeseries      | out.electricity.cooling.energy_consumption  | double    |       |                   |                         |
| timeseries      | out.electricity.cooling.energy_consumption  | double    |       |                   |                         |
| timeseries      | out.electricity.fans.energy_consumption     | double    |       |                   |                         |
| ...             |                                             |           |       |                   |                         |

## enumeration_dictionary.tsv

Expands the definitions of the enumerations used in the metadata files.

| enumeration | enumeration_description                                                             |
|----------   |--------------                                                                       |
| Success     | Simulation completed successfully, results should exist for this simulation         |
| Fail        | Simulation failed, no results or timeseries data should exist for this simulation   |
| 90.1-2004   | ASHRAE 90.1-2004                                                                    |
| 90.1-2007   | ASHRAE 90.1-2007                                                                    |
| ...         | ASHRAE 90.1-2007                                                                    |


## upgrade_dictionary.tsv

Expands the definitions of the upgrades.

| upgrade_id| upgrade_name    | upgrade_description                                   |
|---------- |--------------   |------                                                 |
| 0         | Baseline        | Baseline existing building stock                      |
| 1         | Low-e Windows   | Low-emissivity windows key assumptions here           |
| ...       |                 |                                                       |

## timeseries_aggregates/by_<geography>/<geography_id>-<building_type>.csv

Aggregate timeseries data by end use.

Key information about the aggregates in the columns includes:
- models_used: the number of simulated dwelling units (residential) or buildings (commercial) included in the aggregation
- units_represented (residential): the number of dwelling units represented by these models and this aggregate.
- floor_area_represented (commercial): the floor area represented by these models and this aggregate, in square feet.

| puma     | in.building_type     | timestamp           | out.electricity.cooling.energy_consumption| out.electricity.fans.energy_consumption | ... |
|---       |---                   |---                  |---                |---                |--- |
| G01000100| FullServiceRestaurant| 2018-01-01 00:15:00 | 9.2340859558096273|0.88880632110450342||
| G01000100| FullServiceRestaurant| 2018-01-01 00:30:00 | 9.8277726324740815|0.88880632110450342||
| G01000100| FullServiceRestaurant| 2018-01-01 00:45:00 | 10.370830768750908|0.88880632110450342||
