import argparse
import inspect
import os
import sys
from nrel_xstock.database import ResstockDatabase, SQLiteDatabase
from nrel_xstock.simulate import OpenStudioModelEditor, Simulator
from nrel_xstock.utilities import read_json

class CityLearnSimulator(Simulator):
    def __init__(self,idd_filepath,idf,epw,ems_objects_to_remove=None,**kwargs):
        super().__init__(idd_filepath,idf,epw,**kwargs)
        self.ems_objects_to_remove = ems_objects_to_remove

    @property
    def ems_objects_to_remove(self):
        return self.__ems_objects_to_remove

    @ems_objects_to_remove.setter
    def ems_objects_to_remove(self,ems_objects_to_remove):
        self.__ems_objects_to_remove = ems_objects_to_remove if ems_objects_to_remove is not None else {}

    def simulate(self,**run_kwargs):
        self.preprocess()
        super().simulate(**run_kwargs)
        # rerun = True

        # while rerun:
        #     try:
        #         rerun = False
        #         super().simulate(**run_kwargs)

        #     except EnergyPlusRunError as e:
        #         rerun = self.__find_ems_objects_to_remove()
                
        #         if not rerun:
        #             raise e
        #         else:
        #             print('Rerunning simulation after removing EMS objects.')

        # print(self.ems_objects_to_remove)
        # assert False      

    # def __find_ems_objects_to_remove(self):
    #     found = False
    #     filepath = os.path.join(self.output_directory,f'{self.simulation_id}.err')
    #     errors = get_data_from_path(filepath)
    #     errors = errors.split('\n')
    #     errors = [row.strip() for row in errors]
    #     severe_ixs = [i for i, row in enumerate(errors) if row.startswith('** Severe  **')]
    #     severe = []

    #     for ix in severe_ixs:
    #         severe.append(errors[ix])

    #         for i in range(ix+1,len(errors)):
    #             row = errors[i]

    #             if row.startswith('**   ~~~   **'):
    #                 severe.append(row)
    #             else:
    #                 break

    #     errors = ' '.join(severe)
    #     errors = errors.split('** Severe  **')
    #     errors = [row for row in errors if 'EnergyManagementSystem' in row]
    #     ems_objects = {}

    #     for row in errors:
    #         contents = row.split(' ')
    #         obj = [content for content in contents if 'EnergyManagementSystem' in content]
            
    #         if len(obj) == 1:
    #             found = True
    #             obj = obj[0]
    #             key, value = obj.split('=')[0], obj.split('=')[-1]
    #             ems_objects[key] = ems_objects[key] + [value] if key in ems_objects.keys() else [value]
    #         elif len(obj) == 0:
    #             continue
    #         else:
    #             raise Exception(f'Unidentifiable severe error: {row}')

    #     if found:
    #         print(ems_objects)
    #         ems_objects_to_remove = self.ems_objects_to_remove
    #         idf = self.get_idf_object()
            
    #         for key, value in ems_objects.items():
    #             objects = [obj for obj in idf.idfobjects[key] if obj.Name.upper() in value]
                
    #             for obj in objects:
    #                 idf.removeidfobject(obj)

    #             ems_objects_to_remove[key] = ems_objects_to_remove[key] + value if key in ems_objects_to_remove.keys() else value
            
    #         self.ems_objects_to_remove = ems_objects_to_remove
    #         self.idf = idf.idfstr()
            
    #     else:
    #         pass
        
    #     return found


    def get_simulation_result(self,query_filepath):
        with open(query_filepath,'r') as f:
            query = f.read()

        database = SQLiteDatabase(os.path.join(self.output_directory,f'{self.simulation_id}.sql'))
        data = database.query_table(query)
        # Parantheses in column names changed to square braces to match CityLearn format
        # SQLite3 ignores square braces in column names so parentheses used as temporary fix. 
        data.columns = [c.replace('(','[').replace(')',']') for c in data.columns]
        return data

    def preprocess(self):
        idf = self.get_idf_object()
        # *********** update timestep ***********
        # obj = idf.idfobjects['Timestep'][0]
        # obj.Number_of_Timesteps_per_Hour = 1

        # *********** update output variables ***********
        output_variables = {
            'Equipment Electric Power': ['Lights Electricity Energy','Electric Equipment Electricity Energy',],
            'Indoor Temperature [C]':['Zone Air Temperature',],
            'Indoor Relative Humidity [%]':['Zone Air Relative Humidity'],
            'Average Unmet Cooling Setpoint Difference [C]':['Zone Thermostat Cooling Setpoint Temperature'],
            'Average Unmet Heating Setpoint Difference [C]':['Zone Thermostat Heating Setpoint Temperature'],
            'DHW Heating [kWh]':['Water Heater Heating Energy','Water Heater Tank Temperature','Water Heater Water Volume','Water Heater Runtime Fraction'],
            'Cooling Load [kWh]':['Zone Ideal Loads Zone Total Cooling Energy','Zone Predicted Sensible Load to Setpoint Heat Transfer Rate','Zone Predicted Moisture Load Moisture Transfer Rate',],
            'Heating Load [kWh]':['Zone Ideal Loads Zone Total Heating Energy',],
        }
        idf.idfobjects['Output:Variable'] = []

        for _, value in output_variables.items():
            for output_variable in value:
                obj = idf.newidfobject('Output:Variable')
                obj.Variable_Name = output_variable

        self.idf = idf.idfstr()

def __insert(**kwargs):
    filters = read_json(kwargs.pop('filters_filepath')) if kwargs.get('filters_filepath') is not None else None
    database = ResstockDatabase(kwargs.pop('filepath'))
    kwargs = {**kwargs,'filters':filters}
    database.insert_dataset(**kwargs)
    
def __simulate(**kwargs):
    database = ResstockDatabase(kwargs['filepath'])
    idd_filepath = kwargs['idd_filepath']
    metadata_ids = kwargs.get('metadata_ids') if kwargs.get('metadata_ids') is not None else\
        database.query_table(f"SELECT id FROM metadata")['id'].astype(str).tolist()
    root_output_directory = kwargs.get('root_output_directory','')
    simulation_result_query_filepath = os.path.join(os.path.dirname(__file__),'misc/citylearn_simulation_result.sql')

    for i, metadata_id in enumerate(metadata_ids):
        print(f'Simulating metadata_id:{metadata_id} ({i+1}/{len(metadata_ids)})')
        # get input data for simulation
        sim_data = database.query_table(f"SELECT osm, epw FROM building_energy_performance_simulation_input WHERE metadata_id = {metadata_id}")
        osm, epw = sim_data.to_records(index=False)[0]
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
        
        # write
        simulation_result.columns = [c.replace('[','(').replace(']',')') for c in simulation_result.columns]
        simulation_result['metadata_id'] = metadata_id
        database.insert(
            'citylearn_energyplus_simulation_result',
            simulation_result.columns.tolist(),
            simulation_result.values,
            on_conflict_fields=['metadata_id','Month','Hour', 'Day Type']
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
    subparser_insert.set_defaults(func=__insert)

    # simulate
    subparser_simulate = subparsers.add_parser('simulate',description='Run EnergyPlus simulation.')
    subparser_simulate.add_argument('idd_filepath',type=str,help='Energyplus IDD filepath.')
    subparser_simulate.add_argument('-i','--metadata_ids',nargs='+',dest='metadata_ids',help='Metadata IDs of buildings to simulate. Refer to netadata table to locate IDs.')
    subparser_simulate.add_argument('-o','--root_output_directory',type=str,dest='root_output_directory',help='Root directory to store simulation output directory to.')
    subparser_simulate.set_defaults(func=__simulate)
    
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