import variables as var
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression
import numpy as np
import logging
import itertools


def add_areas(flows, areas=None, role=None, admin_level=None):
    """
    Add administrative areas to roles
    flows: flow dataframe -> DataFrame
    areas: area polygons -> GeoDataFrame
    role: role to assign areas -> string
    admin_level: administrative level of areas -> string
    """
    columns = list(flows)

    # join geolocation with area polygons
    flows['wkt'] = gpd.GeoSeries.from_wkt(flows[f'{role}_Location'])
    flows = gpd.GeoDataFrame(flows, geometry='wkt', crs='EPSG:4326')
    flows = gpd.sjoin(flows, areas, how='left', op='within')

    # keep only original dataframe columns & role area
    flows[f'{role}_{admin_level}'] = flows['name']
    columns.append(f'{role}_{admin_level}')
    flows = flows[columns]

    return flows


def compute_trends(df, on=[], values=[], per_months=12):
    """
    Analyse trends for areas on different timeframe
    df: flow dataframe -> DataFrame
    on: properties to groupby -> list
    values: contains list of values for each property to select -> list
    per_months: timeframe to analyse (year=12, quarter=3 etc.) -> int
    """
    # columns to select & groupby
    columns = on.copy()
    columns.extend([
        'MeldPeriodeJAAR',
        'Gewicht_TN'
    ])
    groupby = columns[:-1]

    # split months into periods
    months = list(range(1, 13))
    periods = [
        months[i: i + per_months] for i in range(0, len(months), per_months)
    ]

    # aggregate into periods of months for each year
    df_new = []
    for year in var.YEARS:
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

    # find unique values for properties if not provided
    new_values = []
    for idx, value in enumerate(values):
        if not value:
            value = df_new.drop_duplicates(on[idx])
            value = value[on[idx]].to_list()
        new_values.append(value)

    # retrieve flows for each unique values & run analysis
    tab = "\t"
    with open(f'./test/{on}_trends.csv', mode='w') as file:
        file.write(f'{tab.join(on)}{tab}Change (%)\n')

        # iterate permutations for all properties & values
        for value in itertools.product(*new_values):
            # form conditions & select flows for current permutation
            conditions = []
            for key, val in zip(on, value):
                conditions.append(df_new[key] == val)
            flows = df_new.loc[np.bitwise_and.reduce(conditions)]

            # prepare data & run linear regression
            X, Y = [], []
            for idx, flow in flows.iterrows():
                time = flow['MeldPeriodeJAAR'] * 12 + flow['Periode'] * per_months
                amount = flow['Gewicht_TN']
                X.append(time)
                Y.append(amount)
            X = np.array(X).reshape(-1, 1)
            if Y:
                # linear regression
                reg = LinearRegression().fit(X, Y)

                # compute initial & final amount based on model
                Y_initial = reg.predict(np.array(X[0]).reshape(-1, 1))[0]
                Y_final = reg.predict(np.array(X[-1]).reshape(-1, 1))[0]

                # change relative to initial amount
                change = (Y_final - Y_initial) / Y_initial * 100

                file.write(f'{tab.join(value)}{tab}{change:.1f}\n')


def compute_actions(flows, provincies, gemeenten):
    """
    Compute actions for provincie & gemeenten
    """

    # # add gemeente & provincie to flow origins (herkomst)
    # logging.info("Add gemeente & provincie to flow origins (herkomst)...")
    # flows = add_areas(flows, role='Herkomst', areas=gemeenten, admin_level='Gemeente')
    # flows = add_areas(flows, role='Herkomst', areas=provincies, admin_level='Provincie')
    #
    # # add gemeente & provincie to flows destinations (verwerker)
    # logging.info("Add gemeente & provincie to flows destinations (verwerker)...")
    # flows = add_areas(flows, role='Verwerker', areas=gemeenten, admin_level='Gemeente')
    # flows = add_areas(flows, role='Verwerker', areas=provincies, admin_level='Provincie')
    #
    # # filter flows with origin (herkomst) or destination (verwerker)
    # # within province in study
    # logging.info("Filter flows within province in study...")
    # flows = flows.loc[
    #     (flows['Herkomst_Provincie'] == PROVINCE) |
    #     (flows['Verwerker_Provincie'] == PROVINCE)
    # ]

    # get names of provincie gemeenten
    provincie_gemeenten = gemeenten[gemeenten['parent'] == var.PROVINCE]['name'].to_list()

    # add verwerking to flows
    flows['Verwerking'] = flows['VerwerkingsmethodeCode'].str[0]

    # compute area trends per quarter (3 months)
    logging.info("Compute trends...")
    roles = ['Herkomst', 'Verwerker']
    areas = ['Provincie', 'Gemeente']
    for prop in itertools.product(roles, areas):
        prop = '_'.join(prop)

        # general trends
        compute_trends(flows, on=[prop], values=[provincie_gemeenten], per_months=3)

        # trends per process group (verwerking)
        compute_trends(flows, on=[prop, 'Verwerking'], values=[provincie_gemeenten, []], per_months=3)