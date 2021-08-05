import os
import pandas as pd
import geopandas as gpd

import connections
import config
import utils

def extract(path):

    data= pd.DataFrame()
    for file in os.listdir(path):
        df_tmp = pd.read_csv(os.path.join(path, file))
        data= pd.concat([data, df_tmp])
    
    return data

def transformGenetec(dataGenetec, con, numTech=0, nameTech='Genetec'):
    dataGenetec = dataGenetec.copy()

    # parse genetec data
    dataGenetec.PatrollerId = dataGenetec.PatrollerId.fillna(0)
    dataGenetec.TimestampLocal = pd.to_datetime(dataGenetec.TimestampLocal)
    dataGenetec['NoJourDeLecture'] = (dataGenetec.TimestampLocal.dt.date.diff().dt.days != 0).cumsum()
    dataGenetec.Infraction = dataGenetec.Infraction.map({'N':0, 'O':1}).fillna(0).astype(int)
    dataGenetec[['Latitude', 'Longitude']] = dataGenetec[['Latitude', 'Longitude']].round(10)

    # create DB dataframe
    columns = pd.read_sql(con=con, sql=f'SELECT * FROM {config.TABLE_NAME}')
    data = pd.DataFrame(columns=columns)

    # fill it
    data['SK_D_Vehicule'] =  dataGenetec['PatrollerId']
    data['NoDeTechno'] =  numTech
    data['NoDeJourDeLecture'] =  dataGenetec['NoJourDeLecture']
    data['DateDePassage'] =  dataGenetec['TimestampLocal'].dt.date
    data['InstantDeLecture'] =  dataGenetec['TimestampLocal']
    data['HeureDeLecture'] =  dataGenetec['TimestampLocal'].dt.hour
    data['Latitude'] =  dataGenetec['Latitude']
    data['Longitude'] =  dataGenetec['Longitude']
    data['PointGeo'] =  list(map(
        utils.createWktElement,
        gpd.points_from_xy(dataGenetec['Longitude'], dataGenetec['Latitude'])
    ))
    data['NoDePlaque'] =  dataGenetec['LicensePlate'].str[:15]
    data['Techno'] = nameTech
    data['IndInfraction'] = dataGenetec['Infraction']

    return data

def transformTanary(dataTanary, con, numTech=1, nameTech='Tanary-Creek'):
    dataTanary = dataTanary.copy()

    # parse genetec data
    dataTanary.TIME_SEEN = pd.to_datetime(dataTanary.TIME_SEEN)
    dataTanary['NoJourDeLecture'] = (dataTanary.TIME_SEEN.dt.date.diff().dt.days != 0).cumsum()
    dataTanary.SCAN_SIDE = dataTanary.SCAN_SIDE.map({'R':1, 'L':-1}).fillna(0).astype(int)
    dataTanary['VehId'] = 1 # Tanary does not have any veh id for now

    # create DB dataframe
    columns = pd.read_sql(con=con, sql=f'SELECT * FROM {config.TABLE_NAME}')
    data = pd.DataFrame(columns=columns)

    # fill it
    data['SK_D_Vehicule'] =  dataTanary['VehId']
    data['NoDeTechno'] =  numTech
    data['NoDeJourDeLecture'] =  dataTanary['NoJourDeLecture']
    data['DateDePassage'] =  dataTanary['TIME_SEEN'].dt.date
    data['InstantDeLecture'] =  dataTanary['TIME_SEEN']
    data['HeureDeLecture'] =  dataTanary['TIME_SEEN'].dt.hour
    data['Latitude'] =  dataTanary['LATITUDE']
    data['Longitude'] =  dataTanary['LONGITUDE']
    data['PointGeo'] =  list(map(
        utils.createWktElement,
        gpd.points_from_xy(dataTanary['LONGITUDE'], dataTanary['LATITUDE'])
    ))
    data['NoDePlaque'] =  dataTanary['PLATE']
    data['Techno'] = nameTech
    data['NoPlaceDerivee'] = 'P999'

    return data

if __name__=='__main__':

    # Connect to the DataBase
    engine = utils.getEngine(**connections.LAPI_REV)
    con = engine.connect()

    # Create table if not exist and troncate
    if not utils.checkTableExists(con, config.TABLE_NAME):
        utils.create_table(con, config.TABLE_NAME, config.TABLE_SQL)
    # TODO: Do not truncute and just insert new values
    utils.truncate_table(con, config.TABLE_NAME)

    # ETL for Genetec Data
    dataGenetec = extract(**connections.SOURCES['Genetec'])
    dataLoad = transformGenetec(dataGenetec, con)
    dataLoad.to_sql(config.TABLE_NAME, con=con, index=False, if_exists='append', schema='dbo', dtype={'PointGeo': utils.Geometry})

    # ETL for Tanary-Creek
    dataTanary = extract(**connections.SOURCES['Tanary-Creek'])
    dataLoad = transformTanary(dataTanary, con)
    dataLoad.to_sql(config.TABLE_NAME, con=con, index=False, if_exists='append', schema='dbo', dtype={'PointGeo': utils.Geometry})
