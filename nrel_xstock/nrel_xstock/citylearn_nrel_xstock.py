import os
import random
import pandas as pd
import simplejson as json
from nrel_xstock.database import SQLiteDatabase, XStockDatabase
from nrel_xstock.simulate import OpenStudioModelEditor, Simulator
from nrel_xstock.utilities import read_json, write_data, write_json

class CityLearnSimulator(Simulator):
    def __init__(self,idd_filepath,idf,epw,**kwargs):
        super().__init__(idd_filepath,idf,epw,**kwargs)

    def __get_simulation_result_metadata(self):
        return {
            'Month':{'output_variables':None,'default':None},
            'Hour':{'output_variables':None,'default':None},
            'Day Type':{'output_variables':None,'default':None},
            'Daylight Savings Status':{'output_variables':None,'default':None},
            'Equipment Electric Power [kWh]':{'output_variables':['Lights Electricity Energy','Electric Equipment Electricity Energy',],'default':0},
            'Indoor Temperature [C]':{'output_variables':['Zone Air Temperature',],'default':None},
            'Indoor Relative Humidity [%]':{'output_variables':['Zone Air Relative Humidity'],'default':None},
            'Average Unmet Cooling Setpoint Difference [C]':{'output_variables':['Zone Thermostat Cooling Setpoint Temperature'],'default':0},
            'Average Unmet Heating Setpoint Difference [C]':{'output_variables':['Zone Thermostat Heating Setpoint Temperature'],'default':0},
            'DHW Heating [kWh]':{'output_variables':['Water Heater Heating Energy',],'default':0},
            'Cooling Load [kWh]':{'output_variables':['Zone Predicted Sensible Load to Setpoint Heat Transfer Rate',],'default':0},
            'Heating Load [kWh]':{'output_variables':['Zone Predicted Sensible Load to Setpoint Heat Transfer Rate',],'default':0},
        }

    def get_simulation_result(self,query_filepath):
        with open(query_filepath,'r') as f:
            query = f.read()

        database = SQLiteDatabase(os.path.join(self.output_directory,f'{self.simulation_id}.sql'))
        data = database.query_table(query)
        # Parantheses in column names changed to square braces to match CityLearn format
        # SQLite3 ignores square braces in column names so parentheses used as temporary fix. 
        data.columns = [c.replace('(','[').replace(')',']') for c in data.columns]

        # set defaults
        for key, value in self.__get_simulation_result_metadata().items():
            default = value.get('default',None)

            if key in data.columns and default is not None:
                data[key] = data[key].fillna(default)
            else:
                continue

        return data

    def preprocess(self):
        idf = self.get_idf_object()

        # *********** update output variables ***********
        idf.idfobjects['Output:Variable'] = []
        output_variables = set([
            variable for key, value in self.__get_simulation_result_metadata().items() 
            if value['output_variables'] is not None
            for variable in value['output_variables']
        ])

        for output_variable in output_variables:
            obj = idf.newidfobject('Output:Variable')
            obj.Variable_Name = output_variable

        self.idf = idf.idfstr()

    def get_attributes(self,random_seed=None):
        attributes = read_json(os.path.join(os.path.dirname(__file__),'misc/building_attributes_template.json'))

        # randomize applicable values
        if random_seed is not None:
            random.seed(random_seed)
            attributes['Solar_Power_Installed(kW)'] = attributes['Solar_Power_Installed(kW)']*random.randint(0,10)
            attributes['Battery']['capacity'] = attributes['Battery']['capacity']*random.randint(0,2)
            attributes['Heat_Pump']['technical_efficiency'] = random.uniform(0.2,0.3)
            attributes['Heat_Pump']['t_target_heating'] = random.randint(47,50)
            attributes['Heat_Pump']['t_target_cooling'] = random.randint(7,10)
            attributes['Electric_Water_Heater']['efficiency'] = random.uniform(0.9,1.0)
            attributes['Chilled_Water_Tank']['loss_coefficient'] = random.uniform(0.002,0.01)
            attributes['DHW_Tank']['loss_coefficient'] = random.uniform(0.002,0.01)
        else:
            pass

        return attributes

    def get_state_action_space(self,attributes):
        state_action_space = read_json(os.path.join(os.path.dirname(__file__),'misc/building_state_action_space_template.json'))
        # states
        state_action_space['states']['solar_gen'] = True if attributes['Solar_Power_Installed(kW)'] > 0 else False
        state_action_space['states']['cooling_storage_soc'] = True if attributes['Chilled_Water_Tank']['capacity'] > 0 else False
        state_action_space['states']['dhw_storage_soc'] = True if attributes['DHW_Tank']['capacity'] > 0 else False
        state_action_space['states']['electrical_storage_soc'] = True if attributes['Battery']['capacity'] > 0 else False
        # actions
        state_action_space['actions']['cooling_storage'] = True if attributes['Chilled_Water_Tank']['capacity'] > 0 else False
        state_action_space['actions']['dhw_storage'] = True if attributes['DHW_Tank']['capacity'] > 0 else False
        state_action_space['actions']['electrical_storage'] = True if attributes['Battery']['capacity'] > 0 else False

        return state_action_space

class CityLearnNRELXStock:
    @staticmethod
    def initialize(**kwargs):
        XStockDatabase(
            kwargs['filepath'],
            overwrite=kwargs['overwrite'],
            apply_changes=kwargs['apply_changes']
        )

    @staticmethod
    def insert(**kwargs):
        filters = read_json(kwargs.pop('filters_filepath')) if kwargs.get('filters_filepath') is not None else None
        database = XStockDatabase(kwargs.pop('filepath'))
        kwargs = {key:value for key,value in kwargs.items() if key in ['dataset_type','weather_data','year_of_publication','release']}
        kwargs = {**kwargs,'filters':filters}
        database.insert_dataset(**kwargs)

    @staticmethod    
    def simulate_energyplus(**kwargs):
        database = XStockDatabase(kwargs.pop('filepath'))
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
        output_directory = f'output_{simulation_id}'
        output_directory = os.path.join(root_output_directory,output_directory) if root_output_directory is not None else output_directory
        schedules_filename = 'schedules.csv'
        schedule = database.query_table(f"""SELECT * FROM schedule WHERE metadata_id = {simulation_data['metadata_id']}""")
        schedule = schedule.drop(columns=['metadata_id','day','hour','minute',])
        schedule.to_csv(schedules_filename,index=False)
        schedule.to_csv(os.path.join(output_directory,schedules_filename),index=False) # also store to output directory
         
        # simulate
        osm_editor = OpenStudioModelEditor(simulation_data['osm'])
        idf = osm_editor.forward_translate()
        write_data(simulation_data['osm'],os.path.join(output_directory,f'{simulation_id}.osm'))
        simulator = CityLearnSimulator(idd_filepath,idf,simulation_data['epw'],simulation_id=simulation_id,output_directory=output_directory)
        simulator.preprocess()
        simulator.simulate()
        os.remove(schedules_filename)
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

    @staticmethod
    def get_citylearn_environment(**kwargs):
        database = XStockDatabase(kwargs.pop('filepath'))
        neighborhood = pd.read_csv(kwargs['neighborhood_filepath'])
        output_directory = kwargs.get('output_directory','')
        query = f"""
        SELECT
            d.dataset_type,
            d.weather_data,
            d.year_of_publication,
            d.release,
            m.id AS metadata_id,
            m.bldg_id,
            m.upgrade,
            m.in_ashrae_iecc_climate_zone_2004 AS climate_zone,
            w.id AS weather_id,
            w.epw,
            w.weather_file_tmy3,
            a.attributes,
            s.state_action_space,
                'Building' || '_' || d.dataset_type || '_' || d.weather_data || '_' || d.year_of_publication 
                || '_release_' || d.release || '_' || m.bldg_id || '_' || m.upgrade
            AS simulation_id
        FROM dataset d
        INNER JOIN metadata m ON m.dataset_id = d.id
        INNER JOIN weather w ON
            w.dataset_id = d.id
            AND w.weather_file_tmy3 = m.in_weather_file_tmy3
            AND w.weather_file_latitude = m.in_weather_file_latitude
            AND w.weather_file_longitude = m.in_weather_file_longitude
        INNER JOIN citylearn_building_attributes a ON a.metadata_id = m.id
        INNER JOIN citylearn_building_state_action_space s ON s.metadata_id = m.id
        WHERE
            d.dataset_type IN {tuple(neighborhood['dataset_type'].tolist())}
            AND d.dataset_type IN {tuple(neighborhood['dataset_type'].tolist())}
            AND d.weather_data IN {tuple(neighborhood['weather_data'].tolist())}
            AND d.year_of_publication IN {tuple(neighborhood['year_of_publication'].tolist())}
            AND d.release IN {tuple(neighborhood['release'].tolist())}
            AND m.bldg_id IN {tuple(neighborhood['bldg_id'].tolist())}
            AND m.upgrade IN {tuple(neighborhood['upgrade'].tolist())}
        """
        data = database.query_table(query)
        assert data.shape[0] == neighborhood.shape[0], 'Could not find input data for some buildings.'
        # assert len(data['weather_id'].unique()) == 1, f'Neighborhood has multiple weather files: {data["weather_file_tmy3"].unique()}.'
        os.makedirs(output_directory,exist_ok=True)
        building_attributes = {key:json.loads(value) for key, value in data[['simulation_id','attributes']].to_records(index=False)}
        write_json(os.path.join(output_directory,'building_attributes.json'),building_attributes)
        building_state_action_space = {key:json.loads(value) for key, value in data[['simulation_id','state_action_space']].to_records(index=False)}
        write_json(os.path.join(output_directory,'buildings_state_action_space.json'),building_state_action_space)

        for metadata_id, simulation_id in data[['metadata_id','simulation_id']].to_records(index=False):
            query = f"""
            SELECT
                *
            FROM citylearn_energyplus_simulation_result
            WHERE metadata_id = {metadata_id}
            ORDER BY
                "Month",
                "Day Type",
                "Hour"
            """
            simulation_result = database.query_table(query).drop(columns=['metadata_id'])
            simulation_result.columns = [c.replace('(','[').replace(')',']') for c in simulation_result.columns]
            simulation_result.to_csv(os.path.join(output_directory,building_attributes[simulation_id]['File_Name']),index=False)

        write_data(data['epw'].iloc[0],os.path.join(output_directory,'weather.epw'))