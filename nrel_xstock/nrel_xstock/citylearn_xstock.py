import os
import random
from nrel_xstock.database import SQLiteDatabase
from nrel_xstock.simulate import Simulator
from nrel_xstock.utilities import read_json, unnest_dict

class CityLearnSimulator(Simulator):
    def __init__(self,idd_filepath,idf,epw,**kwargs):
        super().__init__(idd_filepath,idf,epw,**kwargs)

    def simulate(self,**run_kwargs):
        self.preprocess()
        super().simulate(**run_kwargs)

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
                for variable in value['output_variables'] if value['output_variables'] is not None
        ])

        for output_variable in output_variables:
            obj = idf.newidfobject('Output:Variable')
            obj.Variable_Name = output_variable

        self.idf = idf.idfstr()

    def get_attributes(self,random_seed=None,**kwargs):
        attributes = read_json('misc/building_attributes_template.json')

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

        separator = ',' 
        attributes = {**unnest_dict(attributes,separator=separator),**unnest_dict(kwargs,separator=separator)}
        return attributes

    def get_state_action_space(self,attributes):
        state_action_space = read_json('misc/building_state_action_space_template.json')
        # states
        state_action_space['states']['solar_gen'] = True if attributes['Solar_Power_Installed(kW)'] > 0 else False
        state_action_space['states']['cooling_storage_soc'] = True if attributes['Chilled_Water_Tank']['capacity'] > 0 else False
        state_action_space['states']['dhw_storage_soc'] = True if attributes['DHW_Tank']['capacity'] > 0 else False
        state_action_space['states']['electrical_storage_soc'] = True if attributes['Battery']['capacity'] > 0 else False
        # actions
        state_action_space['actions']['cooling_storage'] = True if self.attributes['Chilled_Water_Tank']['capacity'] > 0 else False
        state_action_space['actions']['dhw_storage'] = True if self.attributes['DHW_Tank']['capacity'] > 0 else False
        state_action_space['actions']['electrical_storage'] = True if self.attributes['Battery']['capacity'] > 0 else False

        return state_action_space