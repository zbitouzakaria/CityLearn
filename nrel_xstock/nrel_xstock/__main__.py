import argparse
import inspect
import os
import sys
import simplejson as json
from nrel_xstock.database import ResstockDatabase
from nrel_xstock.simulate import CityLearnSimulator, OpenStudioModelEditor
from nrel_xstock.utilities import read_json

def insert(**kwargs):
    filters = read_json(kwargs.pop('filters_filepath')) if kwargs.get('filters_filepath') is not None else None
    database = ResstockDatabase(
        kwargs.pop('filepath'),
        overwrite=kwargs.pop('overwrite'),
        apply_changes=kwargs.pop('apply_changes')
    )
    kwargs = {key:value for key,value in kwargs.items() if key in ['dataset_type','weather_data','year_of_publication','release']}
    kwargs = {**kwargs,'filters':filters}
    database.insert_dataset(**kwargs)
    
def simulate(**kwargs):
    database = ResstockDatabase(
        kwargs.pop('filepath'),
        overwrite=kwargs.pop('overwrite'),
        apply_changes=kwargs.pop('apply_changes')
    )
    dataset_type = kwargs['dataset_type']
    weather_data = kwargs['weather_data']
    year_of_publication = kwargs['year_of_publication']
    release = kwargs['release']
    bldg_id = kwargs['bldg_id']
    upgrade = kwargs['upgrade']
    idd_filepath = kwargs['idd_filepath']
    root_output_directory = kwargs.get('root_output_directory','')
    simulation_result_query_filepath = os.path.join(os.path.dirname(__file__),'misc/citylearn_simulation_result.sql')
    # get input data for simulation
    simulation_data = database.query_table(f"""
    SELECT 
        i.metadata_id,
        i.bldg_osm AS osm, 
        i.bldg_epw AS epw,
        m.in_pv_system_size AS pv_system_size,
        m.in_ashrae_iecc_climate_zone_2004 AS climate_zone
    FROM building_energy_performance_simulation_input i
    LEFT JOIN metadata m ON m.id = i.metadata_id
    WHERE 
        i.dataset_type = '{dataset_type}'
        AND i.dataset_weather_data = '{weather_data}'
        AND i.dataset_year_of_publication = {year_of_publication}
        AND i.dataset_release = {release}
        AND i.bldg_id = {bldg_id}
        AND i.bldg_upgrade = {upgrade}
    """)
    simulation_data = simulation_data.to_dict(orient='records')[0]
    simulation_id = f'{dataset_type}_{weather_data}_{year_of_publication}_release_{release}_{bldg_id}_{upgrade}'

    print(f'Simulating: {simulation_id}')
    assert simulation_data['osm'] is not None, f'osm not found.'
    assert simulation_data['epw'] is not None, f'epw not found.'
    schedules_filepath = 'schedules.csv'
    schedule = database.query_table(f"""SELECT * FROM schedule WHERE metadata_id = {simulation_data['metadata_id']}""")
    schedule = schedule.drop(columns=['metadata_id','day','hour','minute',])
    schedule.to_csv(schedules_filepath,index=False)
    output_directory = f'output_{simulation_id}'
    output_directory = os.path.join(root_output_directory,output_directory) if root_output_directory is not None else output_directory
        
    # simulate
    osm_editor = OpenStudioModelEditor(simulation_data['osm'])
    idf = osm_editor.forward_translate()
    simulator = CityLearnSimulator(idd_filepath,idf,simulation_data['epw'],simulation_id=simulation_id,output_directory=output_directory)
    simulator.preprocess()
    simulator.simulate()
    os.remove(schedules_filepath)
    simulation_result = simulator.get_simulation_result(simulation_result_query_filepath)
    attributes = simulator.get_attributes(random_seed=simulation_id)
    attributes['File_Name'] = f'Building_{simulation_id}.csv'
    attributes['Climate_Zone'] = simulation_data['climate_zone']
    attributes['Solar_Power_Installed(kW)'] = float(simulation_data['pv_system_size'].split(' ')[0])\
        if simulation_data['pv_system_size'] is not None and simulation_data['pv_system_size'] != 'None' else 0
    state_action_space = simulator.get_state_action_space(attributes)
    
    # write to database
    simulation_result.columns = [c.replace('[','(').replace(']',')') for c in simulation_result.columns]
    simulation_result['metadata_id'] = simulation_data['metadata_id']
    database.insert(
        'citylearn_energyplus_simulation_result',
        simulation_result.columns.tolist(),
        simulation_result.values,
        on_conflict_fields=['metadata_id','Month','Hour', 'Day Type']
    )
    database.insert(
        'citylearn_building_attributes',
        ['metadata_id','attributes'],
        [[simulation_data['metadata_id'],json.dumps(attributes,ignore_nan=True)]],
        on_conflict_fields=['metadata_id']
    )
    database.insert(
        'citylearn_building_state_action_space',
        ['metadata_id','state_action_space'],
        [[simulation_data['metadata_id'],json.dumps(state_action_space,ignore_nan=True)]],
        on_conflict_fields=['metadata_id']
    )

def main():
    parser = argparse.ArgumentParser(prog='nrel_xstock',description='Manage NREL Resstock and Comstock data for CityLearn.')
    parser.add_argument('--version', action='version', version='%(prog)s 0.0.2')
    parser.add_argument("-v", "--verbosity",action="count",default=0,help='increase output verbosity')
    parser.add_argument('filepath',help='Database filepath.')
    parser.add_argument('dataset_type',type=str,choices=['resstock','comstock'],help='Residential or commercial building stock dataset.')
    parser.add_argument('weather_data',type=str,choices=['tmy3','amy2018'],help='Weather file used in dataset simulation.')
    parser.add_argument('year_of_publication',type=int,choices=[2021],help='Year dataset was published.')
    parser.add_argument('release',type=int,choices=[1],help='Dataset release version.')
    parser.add_argument('-o','--overwrite',default=False,action='store_true',dest='overwrite',help='Will overwrite database if it exists.')
    parser.add_argument('-a','--apply_changes',default=False,action='store_true',dest='apply_changes',help='Will apply new changes to database schema.')
    subparsers = parser.add_subparsers(title='subcommands',required=True,dest='subcommands')
    
    # insert
    subparser_insert = subparsers.add_parser('insert',description='Insert dataset into database.')
    subparser_insert.add_argument('-f','--filters_filepath',type=str,dest='filters_filepath',help='Insertion filters filepath where keys are columns in metadata table and values are values found in the columns.')
    subparser_insert.set_defaults(func=insert)
    
    # simulate
    subparser_simulate = subparsers.add_parser('simulate',description='Run building EnergyPlus simulation.')
    subparser_simulate.add_argument('idd_filepath',type=str,help='Energyplus IDD filepath.')
    subparser_simulate.add_argument('bldg_id',type=int,help='bldg_id field value in metadata table.')
    subparser_simulate.add_argument('-u','--upgrade',type=int,default=0,help='upgrade field value in metadata table.')
    subparser_simulate.add_argument('-o','--root_output_directory',type=str,dest='root_output_directory',help='Root directory to store simulation output directory to.')
    subparser_simulate.set_defaults(func=simulate)
    
    args = parser.parse_args()
    arg_spec = inspect.getfullargspec(args.func)
    kwargs = {
        key:value for (key, value) in args._get_kwargs() 
        if (key in arg_spec.args or (arg_spec.varkw is not None and key not in ['func','subcommands']))
    }
    args.func(**kwargs)

if __name__ == '__main__':
    sys.exit(main())