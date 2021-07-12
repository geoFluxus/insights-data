from variables import *
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression
import numpy as np
import logging


def join_areas(flows, areas=None, role=None, admin_level=None):
    """
    Add administrative areas to roles
    """
    role = role.capitalize()
    admin_level = admin_level.capitalize()

    columns = list(flows)

    flows['wkt'] = gpd.GeoSeries.from_wkt(flows[f'{role}_Location'])
    flows = gpd.GeoDataFrame(flows, geometry='wkt', crs='EPSG:4326')
    flows = gpd.sjoin(flows, areas, how='left', op='within')

    flows[f'{role}_{admin_level}'] = flows['name']
    columns.append(f'{role}_{admin_level}')
    flows = flows[columns]

    return flows


def compute_trends():
    """
    Compute trends via Linear Regression
    """


def compute_actions(flows, provincies, gemeenten):
    """
    Compute actions for provincie & gemeenten
    """

    # add gemeente & provincie to flow origins (ontdoener)
    logging.info("Add gemeente & provincie to flow origins (ontdoener)...")
    flows = join_areas(flows, role='ontdoener', areas=gemeenten, admin_level='gemeente')
    flows = join_areas(flows, role='ontdoener', areas=provincies, admin_level='provincie')

    # add gemeente & provincie to flows destinations (verwerker)
    logging.info("Add gemeente & provincie to flows destinations (verwerker)...")
    flows = join_areas(flows, role='verwerker', areas=gemeenten, admin_level='gemeente')
    flows = join_areas(flows, role='verwerker', areas=provincies, admin_level='provincie')

    # filter flows with origin (ontdoener) or destination (verwerker)
    # within province in study
    logging.info("Filter flows within province in study...")
    flows = flows.loc[
        (flows['Ontdoener_Provincie'] == PROVINCE) |
        (flows['Verwerker_Gemeente'] == PROVINCE)
    ]

    # group flows by gemeenten & month
    columns = [
        'Ontdoener_Gemeente',
        'Verwerker_Gemeente',
        'MeldPeriodeMAAND',
        'MeldPeriodeJAAR',
        'Gewicht_TN'
    ]
    groupby = columns[:-1]
    gemeenten_flows = flows[columns].groupby(groupby).sum().reset_index()

    # group flows by provincies & month
    columns = [
        'Ontdoener_Provincie',
        'Verwerker_Provincie',
        'MeldPeriodeMAAND',
        'MeldPeriodeJAAR',
        'Gewicht_TN'
    ]
    groupby = columns[:-1]
    provincies_flows = flows[columns].groupby(groupby).sum().reset_index()

    # print('Start regression...')
    # def linear_regression(row):
    #     X = np.array(row[years]).reshape(-1, 1)
    #     zeros = np.count_nonzero(X == 0)
    #     Y = [2016, 2017, 2018, 2019, 2020]
    #     reg = LinearRegression().fit(X, Y)
    #     return pd.Series([reg.score(X, Y), reg.coef_[0], zeros])
    #
    # # compute trends for gemeente
    # gemeenten_flows[['R2', 'coef', 'zeros']] = gemeenten_flows.apply(linear_regression, axis=1)
    # limit = 0.8
    # increase = (gemeenten_flows['R2'] >= limit) & (gemeenten_flows['coef'] > 0)
    # decrease = (gemeenten['R2'] >= limit) & (gemeenten_flows['coef'] < 0)
    # spike = (gemeenten_flows['R2'] < limit) & (gemeenten_flows['zeros'] == 4)
    # gemeenten_flows.loc[increase, 'tag'] = 'increase'
    # gemeenten_flows.loc[decrease, 'tag'] = 'decrease'
    # gemeenten_flows.loc[spike, 'tag'] = 'spike'

    def linear_regression(df):
        # find unique origin-destination pairs for flows
        pairs = df.drop_duplicates(['Ontdoener_Provincie', 'Verwerker_Provincie'])
        pairs = pairs[['Ontdoener_Provincie', 'Verwerker_Provincie']]

        # retrieve flows for each unique pair & run analysis
        for index, row in pairs.iterrows():
            origin = row['Ontdoener_Provincie']
            destination = row['Verwerker_Provincie']
            flows = df.loc[
                (df['Ontdoener_Provincie'] == origin) &
                (df ['Verwerker_Provincie'] == destination)
            ]
            print(len(flows))

    linear_regression(provincies_flows)