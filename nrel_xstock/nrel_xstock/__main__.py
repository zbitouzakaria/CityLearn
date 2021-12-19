import argparse
import inspect
import os
import sys
import simplejson as json
from nrel_xstock.citylearn_xstock import CityLearnSimulator
from nrel_xstock.database import ResstockDatabase
from nrel_xstock.simulate import OpenStudioModelEditor
from nrel_xstock.utilities import read_json

def insert(**kwargs):
    filters = read_json(kwargs.pop('filters_filepath')) if kwargs.get('filters_filepath') is not None else None
    database = ResstockDatabase(kwargs.pop('filepath'))
    kwargs = {**kwargs,'filters':filters}
    database.insert_dataset(**kwargs)
    
def simulate(**kwargs):
    database = ResstockDatabase(kwargs['filepath'])
    idd_filepath = kwargs['idd_filepath']
    metadata_ids = kwargs.get('metadata_ids') if kwargs.get('metadata_ids') is not None else\
        database.query_table(f"SELECT id FROM metadata")['id'].astype(str).tolist()
    root_output_directory = kwargs.get('root_output_directory','')
    simulation_result_query_filepath = os.path.join(os.path.dirname(__file__),'misc/citylearn_simulation_result.sql')

    for i, metadata_id in enumerate(metadata_ids):
        print(f'Simulating metadata_id:{metadata_id} ({i+1}/{len(metadata_ids)})')
        # get input data for simulation
        sim_data = database.query_table(f"""
            SELECT 
                i.osm, 
                i.epw,
                m.in_pv_system_size
            FROM building_energy_performance_simulation_input i
            LEFT JOIN metadata m ON m.id = i.metadata_id
            WHERE i.metadata_id = {metadata_id}""")
        osm, epw, pv_system_size = sim_data.to_records(index=False)[0]
        assert osm is not None, f'osm for metadata_id:{metadata_id} not found.'
        assert epw is not None, f'epw for metadata_id:{metadata_id} not found.'
        schedules_filepath = 'schedules.csv'
        schedule = database.query_table(f"SELECT * FROM schedule WHERE metadata_id = {metadata_id}")
        schedule = schedule.drop(columns=['metadata_id','day','hour','minute',])
        schedule.to_csv(schedules_filepath,index=False)
        output_directory = f'{metadata_id}_simulation'
        output_directory = os.path.join(root_output_directory,output_directory) if root_output_directory is not None else output_directory
        
        # preprocess osm as needed and translate to idf
        osm_editor = OpenStudioModelEditor(osm)
        idf = osm_editor.forward_translate()
        simulator = CityLearnSimulator(idd_filepath,idf,epw,simulation_id=metadata_id,output_directory=output_directory)
        simulator.preprocess()

        # simulate
        simulator.simulate()
        os.remove(schedules_filepath)
        simulation_result = simulator.get_simulation_result(simulation_result_query_filepath)
        attributes_kwargs = {
            'Solar_Power_Installed(kW)':float(pv_system_size.split(' ')[0]) if pv_system_size is not None else 0
        }
        attributes = simulator.get_attributes(random_seed=metadata_id,**attributes_kwargs)
        state_action_space = simulator.get_state_action_space(attributes)
        
        # write to database
        simulation_result.columns = [c.replace('[','(').replace(']',')') for c in simulation_result.columns]
        simulation_result['metadata_id'] = metadata_id
        database.insert(
            'citylearn_energyplus_simulation_result',
            simulation_result.columns.tolist(),
            simulation_result.values,
            on_conflict_fields=['metadata_id','Month','Hour', 'Day Type']
        )
        database.insert(
            'citylearn_building_attributes',
            ['metadata_id','attributes'],
            [[metadata_id,json.dumps(attributes,ignore_nan=True)]],
            on_conflict_fields=['metadata_id']
        )
        database.insert(
            'citylearn_building_state_action_space',
            ['metadata_id','state_action_space'],
            [[metadata_id,json.dumps(state_action_space,ignore_nan=True)]],
            on_conflict_fields=['metadata_id']
        )

def main():
    parser = argparse.ArgumentParser(prog='nrel_xstock',description='Manage NREL Resstock and Comstock data for CityLearn.')
    parser.add_argument('--version', action='version', version='%(prog)s 0.0.2')
    parser.add_argument("-v", "--verbosity",action="count",default=0,help='increase output verbosity')
    parser.add_argument('filepath',help='Database filepath.')
    subparsers = parser.add_subparsers(title='subcommands',required=True,dest='subcommands')

    # database
    database = ResstockDatabase(None)
    subparser_database = subparsers.add_parser('database',description='Manage SQLite database.')
    database_subparsers = subparser_database.add_subparsers(title='subcommands',required=True,dest='subcommands')
    # build
    subparser_build = database_subparsers.add_parser('build',description='Builds database from internal schema.')
    subparser_build.add_argument('-o','--overwrite',default=False,action='store_true',dest='overwrite',help='Will overwrite database if it exists.')
    subparser_build.add_argument('-a','--apply_changes',default=False,action='store_true',dest='apply_changes',help='Will apply new changes to database schema.')
    subparser_build.set_defaults(func=database.build)
    # insert
    subparser_insert = database_subparsers.add_parser(
        'insert',
        description='Insert Resstock of Comstock dataset into database. See README.md i NREL end-use-load-profiles-for-us-building-stock data lake.'
    )
    subparser_insert.add_argument('dataset_type',type=str,choices=['resstock','comstock'],help='Residential or commercial building stock dataset.')
    subparser_insert.add_argument('weather_data',type=str,choices=['tmy3','amy2018'],help='Weather file used in dataset simulation.')
    subparser_insert.add_argument('year_of_publication',type=int,choices=[2021],help='Year dataset was published.')
    subparser_insert.add_argument('release',type=int,choices=[1],help='Dataset release version.')
    subparser_insert.add_argument('-f','--filters_filepath',type=dict,dest='filters_filepath',help='Insertion filters filepath where keys are columns in metadata table and values are values found in the columns.')
    subparser_insert.set_defaults(func=insert)

    # simulate
    subparser_simulate = subparsers.add_parser('simulate',description='Run EnergyPlus simulation.')
    subparser_simulate.add_argument('idd_filepath',type=str,help='Energyplus IDD filepath.')
    subparser_simulate.add_argument('-i','--metadata_ids',nargs='+',dest='metadata_ids',help='Metadata IDs of buildings to simulate. Refer to netadata table to locate IDs.')
    subparser_simulate.add_argument('-o','--root_output_directory',type=str,dest='root_output_directory',help='Root directory to store simulation output directory to.')
    subparser_simulate.set_defaults(func=simulate)
    
    args = parser.parse_args()
    database.filepath = {key:value for key, value in args._get_kwargs()}['filepath']
    arg_spec = inspect.getfullargspec(args.func)
    kwargs = {
        key:value for (key, value) in args._get_kwargs() 
        if (key in arg_spec.args or (arg_spec.varkw is not None and key not in ['func','subcommands']))
    }
    args.func(**kwargs)

if __name__ == '__main__':
    sys.exit(main())