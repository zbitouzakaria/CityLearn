import gzip
import io
import math
import os
import sqlite3
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3

class SQLiteDatabase:
    def __init__(self,filepath):
        self.filepath = filepath
        self.__register_adapter()
    
    @property
    def filepath(self):
        return self.__filepath
    
    @filepath.setter
    def filepath(self,filepath):
        self.__filepath = filepath

    def __get_connection(self):
        return sqlite3.connect(self.filepath)

    def __validate_query(self,query):
        query = query.replace(',)',')')
        return query
    
    def build(self,schema_filepath=None,overwrite=False,apply_changes=False):
        schema_filepath = os.path.join(os.path.dirname(__file__),'.misc/schema.sql') if schema_filepath is None else schema_filepath

        if os.path.isfile(self.filepath):
            if overwrite:
                os.remove(self.filepath)
            elif not apply_changes:
                return
            else:
                pass
        else:
            pass

        self.execute_sql_from_file(schema_filepath)

    def get_table(self,table_name):
        query = f"SELECT * FROM {table_name}"
        return self.query_table(self.__validate_query(query))

    def query_table(self,query):
        try:
            connection = self.__get_connection()
            df = pd.read_sql(self.__validate_query(query),connection)
        finally:
            connection.close()

        return df

    def get_schema(self):
        try:
            connection = self.__get_connection()
            query = "SELECT * FROM sqlite_master WHERE type IN ('table', 'view')"
            schema = pd.read_sql(self.__validate_query(query),connection)['sql'].tolist()
        finally:
            connection.close()
        
        schema = '\n\n'.join(schema)
        return schema

    def vacuum(self):
        try:
            connection = self.__get_connection()
            connection.execute('VACUUM')
        finally:
            connection.close()

    def drop(self,name,is_view=False):    
        try:
            connection = self.__get_connection()
            query = f"DROP {'VIEW' if is_view else 'TABLE'} IF EXISTS {name}"
            connection.execute(self.__validate_query(query))
        finally:
            connection.close()

    def execute_sql_from_file(self,filepath):
        with open(filepath,'r') as f:
            queries = f.read()
        
        try:
            connection = self.__get_connection()

            for query in queries.split(';'):
                connection.execute(self.__validate_query(query))
        
        finally:
            connection.close()

    def insert_file(self,filepath,table_name,**kwargs):
        df = self.read_file(filepath)
        kwargs['values'] = df.to_records(index=False)
        kwargs['fields'] = kwargs.get('fields',list(df.columns))
        kwargs['table_name'] = table_name
        kwargs['on_conflict_fields'] = kwargs.get('on_conflict_fields',None)
        kwargs['ignore_on_conflict'] = kwargs.get('ignore_on_conflict',False)
        self.insert(**kwargs)

    def read_file(self,filepath):
        reader = {
            'csv':pd.read_csv,
            'pkl':pd.read_pickle,
            'parquet':pd.read_parquet,
        }
        extension = filepath.split('.')[-1]
        method = reader.get(extension,None)

        if method is not None:
            df = method(filepath)
        else:
            raise TypeError(f'Unsupported file extension: .{extension}. Supported file extensions are {list(reader.keys())}')
        
        return df

    def insert(self,table_name,fields,values,on_conflict_fields=None,ignore_on_conflict=False):
        values = [
            [
                None if isinstance(values[i][j],(int,float)) and math.isnan(values[i][j])\
                    else values[i][j] for j in range(len(values[i]))
            ] for i in range(len(values))
        ]
        fields_placeholder = ', '.join([f'\"{field}\"' for field in fields])
        values_placeholder = ', '.join(['?' for _ in fields])
        query = f"""
        INSERT INTO {table_name} ({fields_placeholder}) VALUES ({values_placeholder})
        """

        if on_conflict_fields:
            on_conflict_update_fields = [f'\"{field}\"' for field in fields if field not in on_conflict_fields]
            on_conflict_fields_placeholder = ', '.join([f'\"{field}\"' for field in on_conflict_fields])
            on_conflict_placeholder = f'({", ".join(on_conflict_update_fields)}) = '\
                f'({", ".join(["EXCLUDED." + field for field in on_conflict_update_fields])})'

            if ignore_on_conflict or len(set(fields+on_conflict_fields)) == len(on_conflict_fields):
                query = query.replace('INSERT','INSERT OR IGNORE')
            else:
                query += f"ON CONFLICT ({on_conflict_fields_placeholder}) DO UPDATE SET {on_conflict_placeholder}"
        
        else:
            pass
        
        try:
            connection = self.__get_connection()
            query = self.__validate_query(query)
            connection.executemany(query,values)
            connection.commit()
        finally:
            connection.close()

    def __register_adapter(self):
        sqlite3.register_adapter(np.int64,lambda x: int(x))
        sqlite3.register_adapter(np.int32,lambda x: int(x))
        sqlite3.register_adapter(np.datetime64,lambda x: np.datetime_as_string(x,unit='s').replace('T',' '))

class ResstockDatabase(SQLiteDatabase):
    __ROOT_URL = 'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/'

    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def build(self,overwrite=False,apply_changes=False):
        schema_filepath = os.path.join(os.path.dirname(__file__),'misc/database_schema.sql')
        super().build(schema_filepath,overwrite=overwrite,apply_changes=apply_changes)

    def insert_dataset(self,dataset_type,weather_data,year_of_publication,release,filters):
        dataset = {
            'dataset_type':dataset_type,
            'weather_data':weather_data,
            'year_of_publication':year_of_publication,
            'release':release
        }
        dataset_id = self.__update_dataset_table(dataset)
        self.__update_data_dictionary_table(dataset,dataset_id)
        buildings = self.__update_metadata_table(dataset,dataset_id,filters=filters)

        if buildings is not None:
            self.__update_upgrade_table(dataset,dataset_id,buildings['upgrade'].unique())
            self.__update_spatial_tract_table(dataset,dataset_id,buildings)
            self.__update_weather_table(dataset_id,buildings)
            self.__update_timeseries_table(dataset,buildings)
            self.__update_model_table(dataset,buildings)
            self.__update_schedule_table(buildings)
        else:
            pass
        
    def __update_dataset_table(self,dataset):
        self.insert(
            'dataset',
            list(dataset.keys()),
            [tuple(dataset.values())],
            on_conflict_fields=list(dataset.keys())
        )
        dataset_id = self.query_table(f"""
            SELECT
                id
            FROM dataset
            WHERE
                dataset_type = '{dataset['dataset_type']}'
                AND weather_data = '{dataset['weather_data']}'
                AND year_of_publication = {dataset['year_of_publication']}
                AND release = {dataset['release']}
        """).iloc[0]['id']
        return dataset_id

    def __update_data_dictionary_table(self,dataset,dataset_id):
        data = ResstockDatabase.__download(key='data_dictionary',**dataset)
        data.columns = [c.replace('.','_').lower() for c in data.columns]
        data['dataset_id'] = dataset_id
        self.insert(
            'data_dictionary',
            data.columns.tolist(),
            data.to_records(index=False),
            ['dataset_id','field_location','field_name']
        )

    def __update_metadata_table(self,dataset,dataset_id,filters=None):
        data = ResstockDatabase.__download(key='metadata',**dataset)
        data = data.reset_index(drop=False)

        if filters is not None:
            for column, values in filters.items():
                data = data[data[column].isin(values)].copy()
        else:
            pass

        if data.shape[0] > 0:
            data.columns = [c.replace('.','_').lower() for c in data.columns]
            data['dataset_id'] = dataset_id
            self.insert(
                'metadata',
                data.columns.tolist(),
                data.to_records(index=False),
                ['bldg_id','dataset_id','upgrade']
            )
            buildings = self.query_table(f"""
                SELECT
                    bldg_id,
                    id AS metadata_id,
                    in_county AS county,
                    upgrade,
                    in_nhgis_county_gisjoin,
                    in_nhgis_puma_gisjoin,
                    in_weather_file_latitude,
                    in_weather_file_longitude,
                    in_weather_file_tmy3
                FROM metadata
                WHERE
                    bldg_id IN {tuple(data['bldg_id'].tolist())}
                    AND upgrade IN {tuple(data['upgrade'].unique().tolist())}
                    AND dataset_id = {dataset_id}
            """
            )
            return buildings
        else:
            print('Found no buildings that match filters.')
            return None

    def __update_upgrade_table(self,dataset,dataset_id,upgrade_ids):
        data = ResstockDatabase.__download(key='upgrade_dictionary',**dataset)
        data['dataset_id'] = dataset_id
        data.columns = [c.replace('.','_').lower() for c in data.columns]
        data = data[data['upgrade_id'].isin(upgrade_ids)].copy()
        
        if data.shape[0] > 0:
            self.insert(
                'upgrade_dictionary',
                data.columns.tolist(),
                data.to_records(index=False),
                ['dataset_id','upgrade_id']
            )
        else:
            pass

    def __update_spatial_tract_table(self,dataset,dataset_id,buildings):
        data = ResstockDatabase.__download(key='spatial_tract',**dataset)
        data['dataset_id'] = dataset_id
        columns = ['in_nhgis_county_gisjoin','in_nhgis_puma_gisjoin']
        buildings = buildings.groupby(columns).size()
        buildings = buildings.reset_index(drop=False)[columns].copy()
        buildings.columns = [c.replace('in_','') for c in columns]
        data = pd.merge(data,buildings,on=buildings.columns.tolist(),how='left')
        data.columns = [c.replace('.','_').lower() for c in data.columns]
        self.insert(
            'spatial_tract',
            data.columns.tolist(),
            data.to_records(index=False),
            ['dataset_id','nhgis_tract_gisjoin']
        )

    def __update_weather_table(self,dataset_id,buildings):
        tmy3 = self.download_energyplus_weather_metadata()
        tmy3 = tmy3[tmy3['provider']=='TMY3'].copy()
        tmy3[['longitude','latitude']] = tmy3[['longitude','latitude']].astype(str)
        tmy3 = tmy3.rename(columns={
            'longitude':'in_weather_file_longitude',
            'latitude':'in_weather_file_latitude',
            'title':'energyplus_title',
        })
        buildings = buildings.groupby(
            ['in_weather_file_latitude','in_weather_file_longitude','in_weather_file_tmy3']
        ).size().reset_index().iloc[:,0:-1]
        buildings = pd.merge(buildings,tmy3,on=['in_weather_file_latitude','in_weather_file_longitude'],how='left')
        buildings['count'] = 1
        report = buildings.groupby(
            ['in_weather_file_latitude','in_weather_file_longitude','in_weather_file_tmy3']
        )[['count']].sum().reset_index()
        locations = report['in_weather_file_tmy3'].unique().tolist()
        unknown_locations = buildings[buildings['energyplus_title'].isnull()]['in_weather_file_tmy3'].unique().tolist()
        ambiguous_locations = report[report['count']>1]['in_weather_file_tmy3'].unique().tolist()

        if len(unknown_locations) > 0:
            print(f'{len(unknown_locations)}/{len(locations)} weather_file_tmy3 could not be found.')
        else:
            pass

        if len(ambiguous_locations) > 0:
            print(f'{len(ambiguous_locations)}/{len(locations)} weather_file_tmy3 are ambiguous.')
        else:
            pass
        
        data = buildings[~buildings['in_weather_file_tmy3'].isin(unknown_locations+ambiguous_locations)].copy()

        if len(buildings) > 0:
            session = requests.Session()
            retries = Retry(total=5,backoff_factor=1)
            session.mount('http://',HTTPAdapter(max_retries=retries))
            urllib3.disable_warnings()
            epws = []
            ddys = []

            for epw_url, ddy_url in data[['epw_url','ddy_url']].to_records(index=False):
                response = session.get(epw_url)
                epws.append(response.content.decode())
                response = session.get(ddy_url)
                ddys.append(response.content.decode(encoding='windows-1252'))
            
            data = data[[
                'in_weather_file_latitude',
                'in_weather_file_longitude',
                'in_weather_file_tmy3',
                'energyplus_title',
                'epw_url',
                'ddy_url'
            ]]
            data['epw'] = epws
            data['ddy'] = ddys
            data['dataset_id'] = dataset_id
            data.columns = [c.replace('.','_').replace('in_','').lower() for c in data.columns]
            self.insert(
                'weather',
                data.columns.tolist(),
                data.to_records(index=False),
                ['dataset_id','weather_file_tmy3','weather_file_latitude','weather_file_longitude']
            )

        else:
            pass
        
    def __update_timeseries_table(self,dataset,buildings):
        buildings = buildings[['bldg_id','metadata_id','county','upgrade']].to_records(index=False)
        dataset_url = ResstockDatabase.__get_dataset_url(**dataset)

        for (bldg_id, metadata_id, county, upgrade) in buildings:
            building_path = f'timeseries_individual_buildings/by_county/upgrade={upgrade}/county={county}/{bldg_id}-{upgrade}.parquet'
            url = os.path.join(dataset_url,building_path)
            data = pd.read_parquet(url)
            data = data.reset_index(drop=False)
            data.columns = [c.replace('.','_').lower() for c in data.columns]
            data['metadata_id'] = metadata_id
            self.insert(
                'timeseries',
                data.columns.tolist(),
                data.to_records(index=False),
                ['metadata_id','timestamp']
            )
            break

    def __update_model_table(self,dataset,buildings):
        buildings = buildings[['bldg_id','metadata_id','upgrade']].to_records(index=False)
        dataset_url = ResstockDatabase.__get_dataset_url(**dataset)
        values = []

        for (bldg_id,metadata_id,upgrade) in buildings:
            building_path = f'building_energy_models/bldg{bldg_id:07d}-up{upgrade:02d}.osm.gz'
            url = os.path.join(dataset_url,building_path)
            response = requests.get(url)
            compressed_file = io.BytesIO(response.content)
            decompressed_file = gzip.GzipFile(fileobj=compressed_file,mode='rb')
            osm = decompressed_file.read().decode()
            values.append((metadata_id,osm))
            break
        
        self.insert(
            'model',
            ['metadata_id','osm'],
            values,
            on_conflict_fields=['metadata_id']
        )

    def __update_schedule_table(self,buildings):
        # this is a temporary fix until NREL uploads the schedules.csv files in the data repository
        url = 'https://raw.githubusercontent.com/NREL/resstock/develop/files/8760.csv'
        data = pd.read_csv(url)
        data['timestamp'] = pd.date_range('2019-01-01 00:00:00','2019-12-31 23:00:00',freq='H')
        data = data.set_index('timestamp')
        data = data.resample('900S').ffill()
        data = pd.concat([data]+[data.iloc[-1:] for _ in range(3)])
        data = data.reset_index(drop=False)
        data['day'] = data.index
        data['hour'] = data['timestamp'].dt.hour
        data['minute'] = data['timestamp'].dt.minute
        data = data.drop(columns=['timestamp'])

        for metadata_id in buildings['metadata_id']:
            data['metadata_id'] = metadata_id
            self.insert(
                'schedule',
                data.columns.tolist(),
                data.to_records(index=False),
                on_conflict_fields=['metadata_id','day','hour','minute']
            )
            break

    @classmethod
    def download_metadata(cls,dataset_type,weather_data,year_of_publication,release):
        data = cls.__download(dataset_type,weather_data,year_of_publication,release,'metadata')
        return data
    
    @classmethod
    def __download(cls,dataset_type,weather_data,year_of_publication,release,key):
        downloader = cls.__downloader()
        dataset_url = cls.__get_dataset_url(dataset_type,weather_data,year_of_publication,release)
        url = os.path.join(dataset_url,downloader[key]['url'])
        data = downloader[key]['reader'](url,**downloader[key]['reader_kwargs'])
        return data

    @classmethod
    def __get_dataset_url(cls,dataset_type,weather_data,year_of_publication,release):
        dataset_path = f'{year_of_publication}/{dataset_type}_{weather_data}_release_{release}/'
        return os.path.join(ResstockDatabase.__ROOT_URL,dataset_path)
    
    @classmethod
    def __downloader(cls):
        return {
            'metadata':{
                'url':'metadata/metadata.parquet',
                'reader':pd.read_parquet,
                'reader_kwargs':{}
            },
            'data_dictionary':{
                'url':'data_dictionary.tsv',
                'reader':pd.read_csv,
                'reader_kwargs':{'sep':'\t'}
            },
            'enumeration_dictionary':{
                'url':'enumeration_dictionary.tsv',
                'reader':pd.read_csv,
                'reader_kwargs':{'sep':'\t'}
            },
            'upgrade_dictionary':{
                'url':'upgrade_dictionary.tsv',
                'reader':pd.read_csv,
                'reader_kwargs':{'sep':'\t'}
            },
            'spatial_tract':{
                'url':'geographic_information/spatial_tract_lookup_table.csv',
                'reader':pd.read_csv,
                'reader_kwargs':{'sep':','}
            },
        }

    @classmethod
    def download_energyplus_weather_metadata(cls):
        url = 'https://raw.githubusercontent.com/NREL/EnergyPlus/develop/weather/master.geojson'
        response = requests.get(url)
        response = response.json()
        features = response['features']
        records = []

        for feature in features:
            title = feature['properties']['title']
            epw_url = BeautifulSoup(feature['properties']['epw'],'html.parser').find('a')['href']
            ddy_url = BeautifulSoup(feature['properties']['ddy'],'html.parser').find('a')['href']
            longitude, latitude = tuple(feature['geometry']['coordinates'])
            region = epw_url.split('/')[3]
            country = epw_url.split('/')[4]
            state = epw_url.split('/')[5]
            station_id = title.split('.')[-1].split('_')[0]
            provider = title.split('.')[-1].split('_')[-1]

            records.append({
                'title':title,
                'region':region,
                'country':country,
                'state':state,
                'station_id':station_id,
                'provider':provider,
                'epw_url':epw_url,
                'ddy_url':ddy_url,
                'longitude':longitude,
                'latitude':latitude,
            })

        data = pd.DataFrame(records)
        return data