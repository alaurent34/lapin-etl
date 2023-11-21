import os
import urllib
import requests
import json
import pandas as pd
import geopandas as gpd

from lapin.enhancement.core import Enhancer
from lapin.enhancement.data import read_data, getEngine
import lapin.config
import lapin.analysis.constants

LAST_REVERTED_DATA = '2023-10-15 20:00:52'
LAST_DATA_TO_REVERT = '2023-10-18T13:40:00'

def enhance_bis(data, data_config, roads, roads_config, shape, save_path='./output/all_2023_data/cache',
                osrmhost='https://amd-osrm-demo1.canadaeast.azurecontainer.io:443'):
    """ Duplicata function to correct all 2023 data

    Parameters
    ----------
    data: pandas.DataFrame
        Raw data collected through LAPI systems.
    data_config: dict
        Configuration for the names of the mandatory columns of raw data.
    roads: geopandas.GeoDataFrame
        Data of the roads centerline. We use the geobase of Montreal in our project.
    roads_config: dict
        Configuration for the names of the mandatory columns of roads centerline.
    geodouble: geopandas.GeoDataFrame
        Data representing the curbside of the roads centerlines.
    shape: geopandas.GeoDataFrame
        Delimitation of the study.
    save_path: string
        Ouptut path for the cache.
    osrmhost: string (Default: 'http://localhost:5000')
        URL of the OSRM engine.

    Returns
    -------
    enhanced_data: pandas.DataFrame
        Enhanced dataset of the raw data.

    Todo
    ----
    1. When we will be able to determine the side of the street with accuracy, we'll need
       to compute that before the laps and then compute laps on segment and side of street.
       Also, we need to ensure that lap smoothing work on side_of_street too. Especially
       with dual sided routes.
    2. If we're able to know on wich segments the LAPI's car was, then we should try to remove
       hits reported on wrong segment. Thoose erroned hit will reduce the mean occupancy.
    """
    try:
        data_enhanced = pd.read_csv(save_path+'/data_enhanced.csv')
        print('Getting data from cached!')
    except FileNotFoundError:
        # instanciation de osrm
        osrmhost = osrmhost

        # enhancer
        enhancer = Enhancer(data, **data_config, save_path=save_path)

        # mapmatching
        print('MapMatching...')
        enhancer.mapmatching(osrmhost)

        # linear referencing
        print('Linear Referencing...')
        data_enhanced = enhancer.linear_referencing(
            roads=roads,
            roads_id_col=roads_config['roads_id_col'],
            mapmatching=True,
            shape=shape
        )
        # cleaning after geo_referencing
        data_enhanced = enhancer.remove_points_far_from_road(
            roads=roads,
            roads_id_col=roads_config['roads_id_col'],
            distance=10
        )

        print('Lap computing...')
        # lapi collection timeframes
        # should be done in last to prevent non sequential lap_id
        data_enhanced = enhancer.lapi_lap_sequences(delta_treshold=120)
        print('Car direction along street...')
        # get direction of the LPD veh on the street
        data_enhanced = enhancer.get_car_direction_along_street(roads=roads)

        # check
        enhancer.tests()

        print('Saving...')
        # saving
        data_enhanced.to_csv(os.path.join(save_path, 'data_enhanced.csv'), index=False)

    return data_enhanced


def load_mapmatched(data_matched, engine):

    data_matched.datetime = pd.to_datetime(data_matched.datetime)
    data_matched.lap_time = pd.to_datetime(data_matched.lap_time)
    data_matched.datetime = data_matched.datetime.dt.tz_localize('America/Montreal')
    data_matched.lap_time = data_matched.lap_time.dt.tz_localize('America/Montreal')
    data_matched = data_matched.rename(
        columns=
        {
            'id_point':'sk_f_lect',
            'uuid':'sk_d_vehicule',
            'datetime':'horodatage',
            'lat':'latitude',
            'lng':'longitude',
            'is_infraction': 'ind_infraction',
            'plate_state': 'province_plaque',
            'point_on_segment': 'distance_sur_segment',
            'lap': 'tour',
            'lap_id' : 'id_tour',
            'lap_time': 'horodatage_tour',
        }
    )
    data_matched = data_matched.set_index('sk_f_lect').sort_index()
    data_matched.to_sql(
        name='donnees_lapi_mapmatchees',
        con=engine,
        schema='analyses',
        if_exists='replace',
        index=True
    )


def load_roads(roads, engine):

    roads = roads.rename(columns=lambda x: x.lower()).rename(columns={'id_trc':'segment'})
    roads.to_postgis(
        name='roads',
        con=engine,
        schema='analyses',
        if_exists='replace',
    )

def update_side_of_street(engine):
    sql = f"""
    WITH lapi_data_mtm8 as (
    SELECT
        data.*,
        ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 32188) as geom
    FROM analyses.donnees_lapi_mapmatchees as data
    WHERE 1=1
        AND sk_d_vehicule = 'c8245d24-d8cb-4de8-adfc-ac3d61018de6'
        AND horodatage at time zone 'America/Montreal' < '{LAST_DATA_TO_REVERT}'
        AND horodatage at time zone 'America/Montreal' > '{LAST_REVERTED_DATA}'
    ),
    roads_mtm8 as (
    SELECT
        roads.*,
        ST_Transform(geometry, 32188) as geom
    FROM analyses.roads

    )
    , point_corrected as (
    SELECT
        sk_f_lect,
        d.horodatage at time zone 'America/Montreal',
        ST_Transform(
        ST_Translate(
            d.geom,
            (ST_X(ST_LineInterpolatePoint(r.geom, ST_LineLocatePoint(r.geom, d.geom))) - ST_X(d.geom))*2,
            (ST_Y(ST_LineInterpolatePoint(r.geom, ST_LineLocatePoint(r.geom, d.geom))) - ST_Y(d.geom))*2
        ),
        4326) as geom
    FROM lapi_data_mtm8 d
    LEFT JOIN roads_mtm8 r ON d.segment = r.segment
    )
    UPDATE analyses.donnees_lapi as orig
    SET
    geom_point = corr.geom,
    longitude = ST_X(corr.geom),
    latitude = ST_Y(corr.geom)
    FROM point_corrected as corr
    WHERE orig.sk_f_lect = corr.sk_f_lect
    """
    with engine.connect() as conn:
        t = conn.begin()
        conn.execute(sql)
        t.commit()


def get_open_data_mtl(id_dataset):

    url = f'https://donnees.montreal.ca/api/3/action/datastore_search?resource_id={id_dataset}'
    file = requests.get(url)
    if file.status_code != 200:
        raise urllib.error.HTTPError(
            code=file.status_code,
            url=file.url,
            hdrs=file.headers,
            fp='',
            msg=file.json()['error']['message']
        )
    data = file.json()

    return gpd.GeoDataFrame(data['result']['records'])

def main():

    # get connection setting
    lapin_con = lapin.config.LAPI_CONNECTION
    sql = (
            lapin.config.DEFAULT_SQL +
            'AND horodatage_lecture at time zone '+
            f'\'America/Montreal\' > \'{LAST_REVERTED_DATA}\' '+
            ' AND horodatage_lecture at time zone '+
            f'\'America/Montreal\' <= \'{LAST_DATA_TO_REVERT }\' '
    )
    lapin_con['sql'] = sql

    engine = getEngine(**lapin_con)

    # extract data
    data = read_data(lapin_con)
    roads = get_open_data_mtl('cdf491fa-bf3d-4eb1-8692-c9aa1df462cd')
    roads = gpd.read_file('~/Downloads/geobase.json')
    delim = gpd.GeoDataFrame(
        data=data[data.lat != 0],
        geometry=gpd.points_from_xy(data[data.lat != 0].lng, data[data.lat != 0].lat),
        crs='epsg:4326'
    ).geometry.unary_union.convex_hull
    delim = gpd.GeoDataFrame(geometry=[delim], crs='epsg:4326')

    # mapmatch
    cache_path = os.path.join('./output/all_2023_data/', 'cache')
    data_matched = enhance_bis(
        data=data,
        data_config=lapin.config.DATA_CONFIG,
        roads=roads,
        roads_config=lapin.config.ROADS_CONFIG,
        shape=delim,
        save_path=cache_path
    )

    # load extracted data prior to transform
    load_mapmatched(data_matched, engine)
    load_roads(roads, engine)

    # tranform data
    update_side_of_street(engine)

    return engine

if __name__ == '__main__':
    _ = main()

    # test = gpd.read_postgis('select d.*, geom_point as geom from analyses.donnees_lapi d where date_lecture = \'2023-06-13\'', con=engine)
    # test.crs = 'epsg:4326'
    # test.drop(columns=['horodatage_lecture', 'geom_point', 'date_lecture']).explore()


