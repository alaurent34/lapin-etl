import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd

import connections
import querry
import runtime_conf
import utils


def extract(path, last_date, date_col='TimestampLocal'):
    """ Assume that you have only one day per file
    """
    data = pd.DataFrame()
    for file in os.listdir(path):
        try:
            date = pd.read_csv(os.path.join(path, file), nrows=1)
            if date.empty:
                continue
            date = date[date_col].iloc[0]
            date = pd.to_datetime(date).tz_localize('America/Montreal')
            if date < last_date:
                continue

            df_tmp = pd.read_csv(os.path.join(path, file))
            data= pd.concat([data, df_tmp])
        except pd.errors.EmptyDataError:
            pass

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

def checkExistsOrCreate(con, tableName, tableDefinition):
    # Create table if not exist and troncate
    if not utils.checkTableExists(con, tableName):
        utils.create_table(con, tableName, tableDefinition)

def prepareTable(con, tableName, tableDefinition, truncate=False, techno='Genetec'):

    date = pd.Timestamp('2021-01-01', tz='America/Montreal')
    # Create table if not exist and troncate
    if not utils.checkTableExists(con, tableName):
        utils.create_table(con, tableName, tableDefinition)
    else:
        date = utils.get_last_data_inserted(con, tableName, techno, 'horodatage_lecture')
        date = date.tz_localize('America/Montreal')
    if truncate:
        date = pd.Timestamp('2021-01-01', tz='America/Montreal')

    return date

def transformGenetec(dataGenetec, con, last_date, numTech=0, nameTech='Genetec'):
    dataGenetec = dataGenetec.copy()

    if dataGenetec.empty:
        print('No data to load.')
        sys.exit(1)

    # ensure localtimestamp is for Montreal
    dataGenetec['TimestampUtc'] = pd.to_datetime(dataGenetec['TimestampUtc'])
    dataGenetec['TimestampLocal'] = dataGenetec['TimestampUtc'].dt.tz_localize('utc').dt.tz_convert('America/Montreal')

    # delete row already in datawarehouse
    dataGenetec = dataGenetec[dataGenetec.TimestampLocal > last_date]

    if dataGenetec.empty:
        return dataGenetec

    # parse genetec data
    dataGenetec.PatrollerId = dataGenetec.PatrollerId.fillna(0)
    dataGenetec.Infraction = dataGenetec.Infraction.map({'N':0, 'O':1}).fillna(0).astype(int)
    dataGenetec[['Latitude', 'Longitude']] = dataGenetec[['Latitude', 'Longitude']].round(10)
    dataGenetec['NoJourDeLecture'] = (dataGenetec.TimestampLocal.dt.date.diff().dt.days != 0).cumsum()

    # create DB dataframe
    columns = pd.read_sql(con=con, sql=f'SELECT * FROM analyses.{querry.LECTURE_TABLE_NAME} LIMIT 1').columns
    data = pd.DataFrame(columns=columns)

    #if dataGenetec['LicensePlate'].isna().any():
        #print(dataGenetec[dataGenetec['LicensePlate'].isna()])

    # fill it
    data['sk_d_vehicule'] =  dataGenetec['PatrollerId']
    data['no_techno'] =  numTech
    data['no_jour_lecture'] =  dataGenetec['NoJourDeLecture']
    data['date_lecture'] =  dataGenetec['TimestampLocal'].dt.date
    data['horodatage_lecture'] =  dataGenetec['TimestampLocal']
    data['heure_lecture'] =  dataGenetec['TimestampLocal'].dt.hour
    data['latitude'] =  dataGenetec['Latitude']
    data['longitude'] =  dataGenetec['Longitude']
    data['geom_point'] = gpd.points_from_xy(dataGenetec['Longitude'], dataGenetec['Latitude'])
    data['plaque'] =  dataGenetec['LicensePlate'].str[:15].fillna('NULL')
    data['techno'] = nameTech
    data['ind_infraction'] = dataGenetec['Infraction']
    if 'LicensePlateState' in dataGenetec.columns:
        data['province_plaque'] = dataGenetec['LicensePlateState']
    else:
        data['province_plaque'] = np.nan

    data = data.drop(columns='sk_f_lect', errors='ignore')

    data = gpd.GeoDataFrame(data, geometry='geom_point', crs='epsg:4326')
    return data

def transformTanary(dataTanary, con, numTech=1, nameTech='Tanary-Creek'):
    dataTanary = dataTanary.copy()

    # parse genetec data
    dataTanary.TIME_SEEN = pd.to_datetime(dataTanary.TIME_SEEN)
    dataTanary['NoJourDeLecture'] = (dataTanary.TIME_SEEN.dt.date.diff().dt.days != 0).cumsum()
    dataTanary.SCAN_SIDE = dataTanary.SCAN_SIDE.map({'R':1, 'L':-1}).fillna(0).astype(int)
    dataTanary['VehId'] = 1 # Tanary does not have any veh id for now

    # create DB dataframe
    columns = pd.read_sql(con=con, sql=f'SELECT * FROM {querry.LECTURE_TABLE_NAME}').columns
    data = pd.DataFrame(columns=columns)

    # fill it
    data['sk_d_vehicule'] =  dataTanary['VehId']
    data['no_techno'] =  numTech
    data['no_jour_lecture'] =  dataTanary['NoJourDeLecture']
    data['date_lecture'] =  dataTanary['TIME_SEEN'].dt.date
    data['horodatage_lecture'] =  dataTanary['TIME_SEEN']
    data['heure_lecture'] =  dataTanary['TIME_SEEN'].dt.hour
    data['latitude'] =  dataTanary['LATITUDE']
    data['longitude'] =  dataTanary['LONGITUDE']
    data['geom_point'] = gpd.points_from_xy(dataTanary['LONGITUDE'], dataTanary['LATITUDE'])
    data['plaque'] =  dataTanary['PLATE']
    data['techno'] = nameTech
    data['no_place_derivee'] = np.nan
    data['province_plaque'] = np.nan

    data = data.drop(columns='sk_f_lect', errors='ignore')

    data = gpd.GeoDataFrame(data, geometry='geom_point', crs='epsg:4326')
    return data

def transformSaaqRequest(data):
    data = data.copy()

    data = data[['id_collecte', 'plaque']].copy()
    data = data.drop_duplicates()
    data.rename(columns={'id_collecte':'IdDeProjet', 'plaque':'NoDePlaque'}, inplace=True)
    return data

def loadSaaq(con, data, idProjet):
    data = data.copy()
    # assert that data are not doublons
    assert data.shape[0] == data.NoDePlaque.unique().shape[0], 'There is some doublons'
    # filter plates already stored for this querry
    dataInStore = pd.read_sql(con=con, sql=f'SELECT * FROM {querry.SAAQ_TABLE_NAME} WHERE IdDeProjet={idProjet}')
    data = data[~data['NoDePlaque'].isin(dataInStore['NoDePlaque'])]
    # Store data
    data.to_sql(querry.SAAQ_TABLE_NAME, con=con, index=False, if_exists='append', schema='analyses')

    return data

def exportSaaqFile(con, collecteConf, platesToQuerry=[]):
    data = pd.read_sql(con=con, sql=f'SELECT * FROM {querry.SAAQ_TABLE_NAME} WHERE IdDeProjet={collecteConf["id"]}')
    data = data[['IdDePlaque', 'NoDePlaque']].copy()
    if platesToQuerry:
        data = data[data.NoDePlaque.isin(platesToQuerry)]
    data.rename(columns={'NoDePlaque':'NO_PLAQ', 'IdDePlaque':'champ2'}, inplace=True)
    data.reindex(columns=['NO_PLAQ', 'champ2'])

    path = os.path.join(collecteConf['projectPath'], '03_Travail/33_Préliminaires/')
    os.makedirs(path, exist_ok=True)
    data.to_excel(os.path.join(path, f'{collecteConf["id"]}_{collecteConf["noDemande"]}_plaques.xlsx'), index=False)

def insertLapiData(db_conf, table_name, table_crea):
    pass

if __name__=='__main__':

    if runtime_conf.LOAD_DATA:
        # Connect to the DataBase
        engine = utils.getEngine(**connections.LAPI_REV)
        # begin transaction
        with engine.connect() as con:
            try:
                transaction = con.begin()
                # create or truncate table
                last_data_date = prepareTable(con, querry.LECTURE_TABLE_NAME, querry.LECTURE_TABLE_SQL, truncate=False, techno='Genetec')

                # ETL for Genetec Data
                print('Genetec ETL')
                print('Last recovered data :', last_data_date)
                ## Extract
                dataGenetec = extract(**connections.SOURCES['Genetec'], last_date=last_data_date)
                print('Data extracted.')
                dataLoad = transformGenetec(dataGenetec, con, last_data_date)
                print('Data Transformed.')
                if dataLoad.empty:
                    print('Nothing to do.')
                    sys.exit(0)
                ## Load
                dataLoad.to_postgis(querry.LECTURE_TABLE_NAME, con=con, index=False, if_exists='append', schema='analyses')
                transaction.commit()
                print('Data Loaded.')

                # EL for Tanary-Creek
                # Ne marche pas, prend trop de temps
                # print('Tanary-Creek ETL')
                # dataTanary = extract(**connections.SOURCES['Tanary-Creek'])
                # dataLoad = transformTanary(dataTanary, con)
                # dataLoad.to_sql(querry.LECTURE_TABLE_NAME, con=con, index=False, if_exists='append', schema='dbo', dtype={'PointGeo': utils.Geometry})
            except:
                transaction.rollback()
                raise

    if runtime_conf.LOAD_SAAQ:
        # Connect to the DataBase
        engine = utils.getEngine(**connections.LAPI_REV)
        with engine.connect() as con:
            try:
                transaction = con.begin()
                # create table
                checkExistsOrCreate(con, querry.SAAQ_TABLE_NAME, querry.SAAQ_TABLE_SQL)
                print('Table Created or done nothing')

                filteredData = extractPlaqueCollecte(engine.connect(), runtime_conf.SAAQ_ETUDE)
                print("Recovered data")
                filteredData = transformSaaqRequest(filteredData)
                print('Transformed')
                loadedData = loadSaaq(con, filteredData, runtime_conf.SAAQ_ETUDE['id'])
                print("Loaded")

                # export SAAQ request file to Excel
                exportSaaqFile(engine.connect(), runtime_conf.SAAQ_ETUDE, loadedData.NoDePlaque.to_list())
                print('End')
                transaction.commit()
            except:
                transaction.rollback()
                raise
