#!/bin/sh
# CONSTANTS (edit accordingly)
DATABASE_FILEPATH="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/data/nrel/database.db"
INSERT_DATASET_TYPE="resstock"
INSERT_WEATHER_DATA="tmy3"
INSERT_YEAR_OF_PUBLICATION="2021"
INSERT_RELEASE="1"
INSERT_FILTERS_FILEPATH="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/CityLearn/nrel_xstock/data/insert_filters.json"
SIMULATE_ENERGYPLUS_PARAMETERS_FILEPATH="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/CityLearn/nrel_xstock/data/neighborhoods/austin_neighborhood.csv"
IDD_FILEPATH="/Applications/EnergyPlus-9-6-0/PreProcess/IDFVersionUpdater/V9-6-0-Energy+.idd"
SIMULATE_ENERGYPLUS_ROOT_OUTPUT_DIRECTORY="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/data/nrel/energyplus_simulation"
NEIGHBORHOOD_FILEPATH=$SIMULATE_ENERGYPLUS_PARAMETERS_FILEPATH
CITYLEARN_INPUT_OUTPUT_DIRECTORY="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/data/nrel/citylearn_input/"

# INITIALIZE DATABASE
python -m nrel_xstock $DATABASE_FILEPATH initialize -a || exit 1

# DOWNLOAD & INSERT DATASET
python -m nrel_xstock $DATABASE_FILEPATH dataset $INSERT_DATASET_TYPE $INSERT_WEATHER_DATA $INSERT_YEAR_OF_PUBLICATION $INSERT_RELEASE insert -f $INSERT_FILTERS_FILEPATH || exit 1

# ENERGYPLUS SIMULATION
OLDIFS=$IFS
IFS=','
{
    read
    while read -r dataset_type weather_data year_of_publication release bldg_id upgrade
    do
        python -m nrel_xstock $DATABASE_FILEPATH dataset $dataset_type $weather_data $year_of_publication $release simulate_energyplus $IDD_FILEPATH $bldg_id -u $upgrade -o $SIMULATE_ENERGYPLUS_ROOT_OUTPUT_DIRECTORY || exit 1
    done
} < $SIMULATE_ENERGYPLUS_PARAMETERS_FILEPATH
IFS=$OLDIFS

# GET CITYLEARN INPUT DATA
python -m nrel_xstock $DATABASE_FILEPATH citylearn_simulation_input $NEIGHBORHOOD_FILEPATH -o $CITYLEARN_INPUT_OUTPUT_DIRECTORY