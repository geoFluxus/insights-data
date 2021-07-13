from variables import *
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression
import numpy as np
import logging
import math


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

    # # add gemeente & provincie to flow origins (herkomst)
    # logging.info("Add gemeente & provincie to flow origins (herkomst)...")
    # flows = join_areas(flows, role='herkomst', areas=gemeenten, admin_level='gemeente')
    # flows = join_areas(flows, role='herkomst', areas=provincies, admin_level='provincie')
    #
    # # add gemeente & provincie to flows destinations (verwerker)
    # logging.info("Add gemeente & provincie to flows destinations (verwerker)...")
    # flows = join_areas(flows, role='verwerker', areas=gemeenten, admin_level='gemeente')
    # flows = join_areas(flows, role='verwerker', areas=provincies, admin_level='provincie')
    #
    # # filter flows with origin (herkomst) or destination (verwerker)
    # # within province in study
    # logging.info("Filter flows within province in study...")
    # flows = flows.loc[
    #     (flows['Herkomst_Provincie'] == PROVINCE) |
    #     (flows['Verwerker_Provincie'] == PROVINCE)
    # ]

    # get names of provincie gemeenten
    provincie_gemeenten = gemeenten[gemeenten['parent'] == PROVINCE]['name'].to_list()

    # linear regression
    def linear_regression(df, on=None, values=[], per_months=12):
        print(on)

        # aggregate into periods of months for each year
        df_new = []
        for year in YEARS:
            # columns to select & groupby
            columns = [
                on,
                'MeldPeriodeJAAR',
                'Gewicht_TN'
            ]
            groupby = columns[:-1]

            # split months into periods
            months = list(range(1, 13))
            periods = [
                months[i : i + per_months] for i in range(0, len(months), per_months)
            ]

            # aggregate per period & store to new df
            for idx, period in enumerate(periods):
                df_q = df[
                    (df['MeldPeriodeJAAR'] == year) &
                    (df['MeldPeriodeMAAND'].isin(period))
                ]
                df_q = df_q[columns].groupby(groupby).sum().reset_index()
                df_q['Periode'] = idx + 1
                df_new.append(df_q)

        # concatenate for all periods & years
        df_new = pd.concat(df_new)

        # # find unique values for on-property
        if not values:
            values = df_new.drop_duplicates(on)
            values = values[on].to_list()

        # retrieve flows for each unique values & run analysis
        with open(f'./test/{on}_{per_months}.csv', mode='a') as file:
            file.write('area\tslope\n')
            for value in values:
                flows = df_new.loc[df_new[on] == value]
                # X, Y = [], []
                # for idx, flow in flows.iterrows():
                #     time = flow['MeldPeriodeJAAR'] * 12 + flow['Periode'] * per_months
                #     amount = flow['Gewicht_TN']
                #     X.append(time)
                #     Y.append(amount)
                # X = np.array(X).reshape(-1, 1)
                # if Y:
                #     reg = LinearRegression().fit(X, Y)
                #     print(value, math.degrees(math.atan(reg.coef_[0])))
                #     file.write(f'{value}\t{math.degrees(math.atan(reg.coef_[0]))}\n')
                flows.to_csv(f'./test/{on}_{value}_{per_months}.csv', index=False)

    # trend analysis on origin provincies (herkomst)
    # linear_regression(flows, on='Herkomst_Provincie', values=[PROVINCE])
    # linear_regression(flows, on='Verwerker_Provincie', values=[PROVINCE])
    linear_regression(flows, on='Herkomst_Gemeente', values=provincie_gemeenten, per_months=12)
    # linear_regression(flows, on='Verwerker_Gemeente', values=provincie_gemeenten, per_months=12)