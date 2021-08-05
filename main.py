import os
import pandas as pd
import geopandas as gpd

import connections
import querry
import runtime_conf
import utils

def extract(path):

    data= pd.DataFrame()
    for file in os.listdir(path):
        df_tmp = pd.read_csv(os.path.join(path, file))
        data= pd.concat([data, df_tmp])
    
    return data

def extractPlaqueCollecte(con, collecteConf):
    where_cond = " AND "
    for date in collecteConf['datesCollecte']:
        where_cond += querry.DEFAULT_WHERE.format(date['from'], date['to'])
        where_cond += ' OR '
    where_cond += " 1=2 "
    collecte_sql = querry.DEFAULT_LAPI_QUERRY + where_cond + querry.ORDER_BY_SQL

    data = pd.read_sql(con=con, sql=collecte_sql)
    data['id_collecte'] = collecteConf['id']

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
    columns = pd.read_sql(con=con, sql=f'SELECT * FROM {querry.LECTURE_TABLE_NAME}')
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
    columns = pd.read_sql(con=con, sql=f'SELECT * FROM {querry.LECTURE_TABLE_NAME}')
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

def transformSaaqRequest(data):
    data = data.copy()

    data = data[['id_collecte', 'plaque']].copy()
    data = data.drop_duplicates()
    data.rename(columns={'id_collecte':'IdDeProjet', 'plaque':'NoDePlaque'}, inplace=True)
    return data

def prepareTable(con, tableName, tableDefintion, truncate=False):
    # Create table if not exist and troncate
    if not utils.checkTableExists(con, tableName):
        utils.create_table(con, tableName, tableDefintion)
    # TODO: Do not truncute and just insert new values
    if truncate:
        utils.truncate_table(con, tableName)

def exportSaaqFile(con, collecteConf):
    data = pd.read_sql(con=con, sql=f'SELECT * FROM {querry.SAAQ_TABLE_NAME} WHERE IdDeProjet={collecteConf["id"]}')
    data = data[['IdDePlaque', 'NoDePlaque']].copy()
    data.rename(columns={'NoDePlaque':'NO_PLAQ', 'IdDePlaque':'champ2'}, inplace=True)
    data.reindex(columns=['NO_PLAQ', 'champ2'])

    path = os.path.join(collecteConf['projectPath'], '03_Travail/33_Préliminaires/')
    os.makedirs(path, exist_ok=True)
    data.to_excel(os.path.join(path, f'{collecteConf["id"]}_Plaques.xlsx'), index=False)

def insertLapiData(db_conf, table_name, table_crea):
    pass

if __name__=='__main__':

    if runtime_conf.LOAD_DATA:
        # Connect to the DataBase
        engine = utils.getEngine(**connections.LAPI_REV)
        con = engine.connect()

        # create or truncate table
        prepareTable(con, querry.LECTURE_TABLE_NAME, querry.LECTURE_TABLE_SQL, truncate=True)

        # ETL for Genetec Data
        print('Genetec ETL')
        dataGenetec = extract(**connections.SOURCES['Genetec'])
        dataLoad = transformGenetec(dataGenetec, con)
        dataLoad.to_sql(querry.LECTURE_TABLE_NAME, con=con, index=False, if_exists='append', schema='dbo', dtype={'PointGeo': utils.Geometry})

        # ETL for Tanary-Creek
        # Ne marche pas, prend trop de temps
        # print('Tanary-Creek ETL')
        # dataTanary = extract(**connections.SOURCES['Tanary-Creek'])
        # dataLoad = transformTanary(dataTanary, con)
        # dataLoad.to_sql(querry.LECTURE_TABLE_NAME, con=con, index=False, if_exists='append', schema='dbo', dtype={'PointGeo': utils.Geometry})

    if runtime_conf.LOAD_SAAQ:
        # Connect to the DataBase
        engine = utils.getEngine(**connections.LAPI_REV)
        con = engine.connect()

        # create table
        prepareTable(con, querry.SAAQ_TABLE_NAME, querry.SAAQ_TABLE_SQL, truncate=False)

        filteredData = extractPlaqueCollecte(engine.connect(), querry.ONTARIO_1_CONF)
        filteredData = transformSaaqRequest(filteredData)
        filteredData.to_sql(querry.SAAQ_TABLE_NAME, con=engine.connect(), index=False, if_exists='append', schema='dbo')

        # export SAAQ request file to Excel
        exportSaaqFile(engine.connect(), querry.ONTARIO_1_CONF)