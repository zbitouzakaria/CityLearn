import argparse
import inspect
import sys
from nrel_xstock.citylearn_nrel_xstock import CityLearnNRELXStock

def main():
    parser = argparse.ArgumentParser(prog='nrel_xstock',description='Manage NREL Resstock and Comstock data for CityLearn.')
    parser.add_argument('--version', action='version', version='%(prog)s 0.0.2')
    parser.add_argument("-v", "--verbosity",action="count",default=0,help='increase output verbosity')
    parser.add_argument('filepath',help='Database filepath.')
    subparsers = parser.add_subparsers(title='subcommands',required=True,dest='subcommands')
    
    # initialize
    subparser_insert = subparsers.add_parser('initialize',description='Initialize database.')
    subparser_insert.add_argument('-o','--overwrite',default=False,action='store_true',dest='overwrite',help='Will overwrite database if it exists.')
    subparser_insert.add_argument('-a','--apply_changes',default=False,action='store_true',dest='apply_changes',help='Will apply new changes to database schema.')
    subparser_insert.set_defaults(func=CityLearnNRELXStock.initialize)

    # dataset
    subparser_dataset = subparsers.add_parser('dataset',description='Database dataset operations.')
    subparser_dataset.add_argument('dataset_type',type=str,choices=['resstock'],help='Residential or commercial building stock dataset.')
    subparser_dataset.add_argument('weather_data',type=str,choices=['tmy3'],help='Weather file used in dataset simulation.')
    subparser_dataset.add_argument('year_of_publication',type=int,choices=[2021],help='Year dataset was published.')
    subparser_dataset.add_argument('release',type=int,choices=[1],help='Dataset release version.')
    dataset_subparsers = subparser_dataset.add_subparsers(title='subcommands',required=True,dest='subcommands')

    # dataset -> insert
    subparser_insert = dataset_subparsers.add_parser('insert',description='Insert dataset into database.')
    subparser_insert.add_argument('-f','--filters_filepath',type=str,dest='filters_filepath',help='Insertion filters filepath where keys are columns in metadata table and values are values found in the columns.')
    subparser_insert.set_defaults(func=CityLearnNRELXStock.insert)
    
    # dataset -> simulate energyplus
    subparser_simulate_energyplus = dataset_subparsers.add_parser('simulate_energyplus',description='Run building EnergyPlus simulation.')
    subparser_simulate_energyplus.add_argument('idd_filepath',type=str,help='Energyplus IDD filepath.')
    subparser_simulate_energyplus.add_argument('bldg_id',type=int,help='bldg_id field value in metadata table.')
    subparser_simulate_energyplus.add_argument('-u','--upgrade',type=int,default=0,help='upgrade field value in metadata table.')
    subparser_simulate_energyplus.add_argument('-o','--root_output_directory',type=str,dest='root_output_directory',help='Root directory to store simulation output directory to.')
    subparser_simulate_energyplus.set_defaults(func=CityLearnNRELXStock.simulate_energyplus)

    # citylearn simulation input
    subparser_citylearn_simulation_input = subparsers.add_parser('citylearn_simulation_input',description='Database dataset operations.')
    subparser_citylearn_simulation_input.add_argument('neighborhood_filepath',type=str,help='Neighborhood filepath.')
    subparser_citylearn_simulation_input.add_argument('-o','--output_directory',type=str,dest='output_directory',help='Directory to output to.')
    subparser_citylearn_simulation_input.set_defaults(func=CityLearnNRELXStock.get_citylearn_simulation_input)

    args = parser.parse_args()
    arg_spec = inspect.getfullargspec(args.func)
    kwargs = {
        key:value for (key, value) in args._get_kwargs() 
        if (key in arg_spec.args or (arg_spec.varkw is not None and key not in ['func','subcommands']))
    }
    args.func(**kwargs)

if __name__ == '__main__':
    sys.exit(main())