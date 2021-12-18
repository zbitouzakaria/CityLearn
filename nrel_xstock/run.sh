#!/bin/sh
DATABASE_FILEPATH="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/data/nrel/database.db"
FILTERS_FILEPATH="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/CityLearn/nrel_xstock/filters.json"
IDD_FILEPATH="/Applications/EnergyPlus-9-6-0/PreProcess/IDFVersionUpdater/V9-6-0-Energy+.idd"
ROOT_OUTPUT_DIRECTORY="/Users/kingsleyenweye/Desktop/INTELLIGENT_ENVIRONMENT_LAB/citylearn/data/nrel/energyplus_simulation"
python -m nrel_xstock $DATABASE_FILEPATH database build -a
python -m nrel_xstock $DATABASE_FILEPATH database insert resstock tmy3 2021 1 -f $FILTERS_FILEPATH
python -m nrel_xstock $DATABASE_FILEPATH simulate $IDD_FILEPATH -o $ROOT_OUTPUT_DIRECTORY