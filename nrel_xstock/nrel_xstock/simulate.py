from io import StringIO
import os
from eppy.modeleditor import IDF
from openstudio import energyplus, osversion

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
        self.__idf = Simulator.get_data_from_path(idf)

    @epw.setter
    def epw(self,epw):
        epw = Simulator.get_data_from_path(epw)
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
        run_kwargs = self.__get_run_kwargs(**run_kwargs if run_kwargs is not None else {})
        idf = IDF(StringIO(self.idf),self.__epw_filepath)
        idf.run(**run_kwargs)
        os.remove(self.__epw_filepath)

    def __get_run_kwargs(self,**kwargs):
        idf = IDF(StringIO(self.idf))
        idf_version = idf.idfobjects['version'][0].Version_Identifier.split('.')
        idf_version.extend([0] * (3 - len(idf_version)))
        idf_version_str = '-'.join([str(item) for item in idf_version])
        options = {
            'ep_version':idf_version_str,
            'output_prefix':self.simulation_id,
            'output_suffix':'C',
            'output_directory':self.output_directory,
            'readvars':False,
            'expandobjects':True,
            'verbose':'q',
        }
        options = {**options,**kwargs}
        return options

    def __write_epw(self):
        filepath = os.path.join(self.output_directory,'weather.epw')
        
        with open(filepath,'w') as f:
            f.write(self.epw)

        self.__epw_filepath = filepath

    def preprocess(self):
        raise NotImplementedError
    
    @staticmethod
    def osm_to_idf(osm):
        osm = Simulator.get_data_from_path(osm)
        version_translator = osversion.VersionTranslator()
        osm = version_translator.loadModelFromString(osm).get()
        forward_translator = energyplus.ForwardTranslator()
        idf = forward_translator.translateModel(osm)
        idf = str(idf)
        return idf

    @staticmethod
    def get_data_from_path(filepath):
        if os.path.exists(filepath):
            with open(filepath,mode='r') as f:
                data = f.read()
        else:
            data = filepath

        return data

class CityLearnSimulator(Simulator):
    def __init__(self,idd_filepath,idf,epw,ddy=None,**kwargs):
        super().__init__(idd_filepath,idf,epw,**kwargs)

    def simulate(self,**run_kwargs):
        self.preprocess()
        super().simulate(**run_kwargs)

    def preprocess(self):
        idf = IDF(StringIO(self.idf))
        # *********** update timestep ***********
        obj = idf.idfobjects['Timestep'][0]
        obj.Number_of_Timesteps_per_Hour = 1

        # *********** update output variables ***********
        output_variables = {
            'Equipment Electric Power': [
                'Lights Electricity Energy',
                'Electric Equipment Electricity Energy',
            ],
            'Indoor Temperature [C]':[
                'Zone Air Temperature',
            ],
            'Indoor Relative Humidity [%]':[
                'Zone Air Relative Humidity'
            ],
            'Average Unmet Cooling Setpoint Difference [C]':[
                'Zone Thermostat Cooling Setpoint Temperature'
            ],
            'Average Unmet Heating Setpoint Difference [C]':[
                'Zone Thermostat Heating Setpoint Temperature'
            ],
            'DHW Heating [kWh]':[
                'Water Heater Heating Energy'
            ],
            'Cooling Load [kWh]':[
                'Zone Predicted Sensible Load to Cooling Setpoint Heat Transfer Rate',
                'Zone Predicted Moisture Load Moisture Transfer Rate',
            ],
            'Heating Load [kWh]':[
                'Zone Predicted Sensible Load to Heating Setpoint Heat Transfer Rate',
            ],
        }
        idf.idfobjects['Output:Variable'] = []

        for _, value in output_variables.items():
            for output_variable in value:
                obj = idf.newidfobject('Output:Variable')
                obj.Variable_Name = output_variable

        self.idf = idf.idfstr()