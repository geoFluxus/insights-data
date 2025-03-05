import pandas as pd
import numpy as np
import variables as var
from src.analysis import utils
import gc
import json


DATA = {}
LAP = {
    'total_primary_waste': {
        'goal': 61,
        'metrics': {
          "current_year": {
            "year": 2023,
            "value": 61,
            "unit": "Mt"
          },
          "projection": {
            "year": 2029,
            "value": 63,
            "unit": "Mt"
          }
        }
    }
}


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    path = f"{var.INPUT_DIR}/{var.AREA_DIR}/CBS"
    df = pd.read_excel(f"{path}/Huishoudelijk_Gemeenten.xlsx", sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', 0).replace('-', 0)
    df = pd.merge(df, areas, left_on='Gebieden', right_on='Gemeente', how='left')
    columns.append('Provincie')
    df = df[columns]

    # import population data
    population = pd.read_csv(f"{path}/populationNL.csv", delimiter=';')

    # add population
    def add_population(row):
        gemeente, year = row['Gebieden'], row['Perioden']
        res = population[population['Gemeente'] == gemeente][str(year)]
        if not res.empty:
            res = res.values[0]
            return res
        return np.nan
    df['Inwoners'] = df.apply(lambda x: add_population(x), axis=1)

    return df


def add_areas(df, areas=None, role=None):
    """
    Add gemeente & provincie to role based on postcode
    """
    columns = list(df)

    # join on postcode
    df = pd.merge(df, areas, left_on=f'{role}_PC4', right_on='PC4', how='left')
    df[f'{role}_Gemeente'] = df['Gemeente']
    df[f'{role}_Provincie'] = df['Provincie']
    columns.extend([f'{role}_Gemeente', f'{role}_Provincie'])
    df = df[columns]

    return df


def import_lma_flows(areas=None, year=None):
    """
    Import & ready LMA flows for processing
    """
    columns = [
        'EuralCode',
        'VerwerkingsmethodeCode',
        'Gewicht_KG',
        'Herkomst_Postcode',
        'Herkomst_Land',
        'Verwerker_Postcode',
        'Verwerker_Land'
    ]

    # import file
    path = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/LMA/ontvangst/processed"
    print(f'Import {year} data...')
    df = pd.read_csv(f'{path}/ontvangst_{year}_full.csv', usecols=columns, low_memory=False)

    # ewc code as 6-digit
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)

    # add provincie/gemeente on herkomst & verwerker
    for role in ['Herkomst', 'Verwerker']:
        df.loc[df[f'{role}_Land'] == 'NEDERLAND', f'{role}_PC4'] = df[f'{role}_Postcode'].str[:4]
        df = add_areas(df, areas=areas, role=role)

    return df


def compute_lma_goal(df,
                     role=None,
                     level=None,
                     areas=None,
                     apply=None,
                     year=None,
                     reference=None):
    """
    Compute LMA waste
    """
    columns = [
        f'{role}_{level}',
        'Gewicht_KG'
    ]

    # apply filter function
    if apply: df = apply(df)

    # compute total
    df = df[columns].groupby(columns[:-1]).sum().reset_index()

    # add missing values
    current_values = df[f'{role}_{level}'].to_list()
    for area in areas:
        if area not in current_values:
            df.loc[len(df)] = [area, 0]

    # add to data
    df[year] = utils.kg_to_unit(df['Gewicht_KG'], unit=var.UNITS['NATIONAL_GOALS_UNIT'])
    df['area'] = df[f'{role}_{level}']
    result = df[['area', year]].sort_values(by='area')
    if reference is not None:
        result[year] = (result[year] / reference[year]) * 100

    return result


def cbs_primary_waste(input, level=None, year=None):
    df = input.copy()
    columns = [
        f"{level}",
        "Gewicht_KG",
        "Inwoners"
    ]
    df['Gewicht_KG'] = df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] * df['Inwoners']
    df = df[columns].groupby(f"{level}").sum().reset_index()

    # add to data
    df[year] = utils.kg_to_unit(
        df['Gewicht_KG'],
        unit=var.UNITS['NATIONAL_GOALS_UNIT']
    )
    df['area'] = df[f'{level}']
    result = df[['area', year]].sort_values(by='area')

    return result


def run():
    # import postcodes
    postcodes = pd.read_csv(f"{var.INPUT_DIR}/Database_LockedFiles/GEODATA/postcodes/{var.POSTCODES}.csv", low_memory=False)
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{var.LEVEL}"] == var.AREA]['Gemeente'].to_list()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {sorted(area_gemeenten)}')

    # import household data
    household_data = import_household_data(areas=gemeenten)
    household_data = household_data.rename(columns={'Gebieden': 'Gemeente'})

    # import data for each year
    ALIAS = "Nederland"
    print(f'\nCompute total primary waste...')
    for year in var.GOALS_YEARS:
        print(f'\nAnalyse {year}...')
        LMA_FLOWS = import_lma_flows(areas=postcodes, year=year)
        CBS_FLOWS = household_data[household_data['Perioden'] == int(year)]

        areas = [var.AREA, ALIAS]
        level = var.LEVEL

        # copy initial dataframes
        lma_flows = LMA_FLOWS.copy()
        cbs_flows = CBS_FLOWS.copy()

        # divide between current area & others
        lma_flows.loc[
            (lma_flows[f'Herkomst_{level}'] != var.AREA) &
            (lma_flows[f'Herkomst_Land'] == 'NEDERLAND'),
            f'Herkomst_{level}'
        ] = ALIAS
        cbs_flows.loc[cbs_flows[f"{var.LEVEL}"] != var.AREA, f"{var.LEVEL}"] = ALIAS

        # total company primary waste (LMA) -> weight
        print('Compute company waste...')
        def total_company_primary_waste(df):
            return df[df['EuralCode'].str[:2] != '19']
        total_company_primary = \
            compute_lma_goal(lma_flows,
                             role='Herkomst',
                             level=level,
                             areas=areas,
                             apply=total_company_primary_waste,
                             year=year)

        # # total household primary waste (CBS) -> weight
        # print('Compute household waste...')
        # total_household_primary = \
        #     cbs_primary_waste(cbs_flows,
        #                       level=level,
        #                       year=year)

        # total primary waste = total company primary waste
        total_primary_waste = total_company_primary
        total_primary_waste['area'] = total_company_primary['area']
        DATA.setdefault('total_primary_waste', []).append(total_primary_waste)

        # clean memory & collect garbage
        del LMA_FLOWS
        gc.collect()

    results = {}
    for field, field_data in DATA.items():
        # sort values for merging
        sorted_data = []
        for data in field_data:
            data = data.sort_values(by=['area']).reset_index()
            sorted_data.append(data)

        # merge all years
        df = pd.concat(sorted_data, axis=1)
        df = df.loc[:, ~df.columns.duplicated()]
        columns = list(df.columns)[1:]  # drop index
        df = df[columns]

        # convert to area values
        years = [col for col in df.columns if col != 'area']
        values = {}
        for idx, row in df.iterrows():
             values[row['area']] = [row[year] for year in years]

        results = {
            **LAP[field],
            'unit': var.UNITS['NATIONAL_GOALS_UNIT'],
            'values': values,
            'years': var.GOALS_YEARS
        }

    return results