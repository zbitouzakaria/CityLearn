from io import StringIO
import os
import random
from eppy.modeleditor import IDF
from openstudio import energyplus, osversion
from nrel_xstock.database import SQLiteDatabase
from nrel_xstock.utilities import get_data_from_path, read_json, write_data

class OpenStudioModelEditor:
    def __init__(self,osm):
        self.osm = osm

    @property
    def osm(self):
        return self.__osm

    @osm.setter
    def osm(self,osm):
        self.__osm = get_data_from_path(osm)

    def forward_translate(self):
        osm = self.__get_model()
        forward_translator = energyplus.ForwardTranslator()
        idf = forward_translator.translateModel(osm)
        idf = str(idf)
        return idf

    def use_ideal_loads_air_system(self):
        # Reference: https://www.rubydoc.info/gems/openstudio-standards/Standard#remove_hvac-instance_method
        osm = self.__get_model()

        # remove air loop hvac
        for air_loop in osm.getAirLoopHVACs():
            air_loop.remove()

        # remove plant loops
        for plant_loop in osm.getPlantLoops():
            shw_use = False

            for component in plant_loop.demandComponents():
                if component.to_WaterUseConnections().is_initialized() or component.to_CoilWaterHeatingDesuperheater().is_initialized():
                    shw_use = True
                    break
                else:
                    pass

            if  not shw_use:
                plant_loop.remove()
            else:
                continue

        # remove vrf
        for ac_refrigerant_flow in osm.getAirConditionerVariableRefrigerantFlows():
            ac_refrigerant_flow.remove()

        for terminal_unit_refrigerant_flow in osm.getZoneHVACTerminalUnitVariableRefrigerantFlows():
            terminal_unit_refrigerant_flow.remove()

        # remove zone equipment
        for zone in osm.getThermalZones():
            for equipment in zone.equipment():
                if not equipment.to_FanZoneExhaust().is_initialized():
                    equipment.remove()
                else:
                    pass

        # remove unused curves
        for curve in osm.getCurves():
            if curve.directUseCount() == 0:
                curve.remove()
            else:
                pass

        # add ideal load system
        for zone in osm.getThermalZones():
            zone.setUseIdealAirLoads(True)
            
        osm = str(osm)
        self.osm = osm

    def __get_model(self):
        version_translator = osversion.VersionTranslator()
        osm = version_translator.loadModelFromString(self.osm).get()
        return osm

class Simulator:
    def __init__(self,idd_filepath,idf,epw,simulation_id=None,output_directory=None):
        self.idd_filepath = idd_filepath
        self.epw = epw
        self.idf = idf
        self.simulation_id = simulation_id
        self.output_directory = output_directory
    
    @property
    def idd_filepath(self):
        return self.__idd_filepath

    @property
    def idf(self):
        return self.__idf

    @property
    def epw(self):
        return self.__epw

    @property
    def simulation_id(self):
        return self.__simulation_id

    @property
    def output_directory(self):
        return self.__output_directory

    @idd_filepath.setter
    def idd_filepath(self,idd_filepath):
        self.__idd_filepath = idd_filepath
        IDF.setiddname(self.idd_filepath)

    @idf.setter
    def idf(self,idf):
        self.__idf = get_data_from_path(idf)

    @epw.setter
    def epw(self,epw):
        epw = get_data_from_path(epw)
        self.__epw = epw

    @simulation_id.setter
    def simulation_id(self,simulation_id):
        self.__simulation_id = simulation_id if simulation_id is not None else 'simulation'

    @output_directory.setter
    def output_directory(self,output_directory):
        self.__output_directory = output_directory if output_directory is not None else 'simulation'

    def simulate(self,**run_kwargs):
        os.makedirs(self.output_directory,exist_ok=True)
        self.__write_epw()
        self.__write_idf()
        run_kwargs = self.__get_run_kwargs(**run_kwargs if run_kwargs is not None else {})
        idf = self.get_idf_object(weather=self.__epw_filepath) 
        idf.run(**run_kwargs)
        os.remove(self.__epw_filepath)

    def __get_run_kwargs(self,**kwargs):
        idf = self.get_idf_object()
        idf_version = idf.idfobjects['version'][0].Version_Identifier.split('.')
        idf_version.extend([0] * (3 - len(idf_version)))
        idf_version_str = '-'.join([str(item) for item in idf_version])
        options = {
            'ep_version':idf_version_str,
            'output_prefix':self.simulation_id,
            'output_suffix':'C',
            'output_directory':self.output_directory,
            'readvars':True,
            'expandobjects':True,
            'verbose':'q',
        }
        options = {**options,**kwargs}
        return options

    def __write_epw(self):
        filepath = os.path.join(self.output_directory,'weather.epw')
        write_data(self.epw,filepath)
        self.__epw_filepath = filepath

    def __write_idf(self):
        filepath = os.path.join(self.output_directory,f'{self.simulation_id}.idf')
        write_data(self.idf,filepath)

    def get_idf_object(self,weather=None):
        return IDF(StringIO(self.idf),weather)

    def preprocess(self):
        raise NotImplementedError

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