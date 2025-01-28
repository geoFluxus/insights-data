import pandas as pd
import variables as var
from src.analysis import utils


DATA = {}


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


def compute_lma_goal(df,
                     role=None,
                     level=None,
                     areas=None,
                     groupby=[],
                     apply=None,
                     year=None):
    """
    Compute LMA waste
    """
    groupby = [
        *groupby,
        f'{role}_{level}',
    ]
    agg = [
        'Gewicht_KG',
    ]
    columns = groupby + agg

    # apply filter function
    if apply: df = apply(df)

    # compute total from groupby
    df = df[columns].groupby(groupby).sum().reset_index()

    # compute total sums for areas
    total_sum = df[[
        f'{role}_{level}',
        'Gewicht_KG'
    ]].groupby(f'{role}_{level}').sum().reset_index()
    total_sum = total_sum.set_index(f'{role}_{level}')['Gewicht_KG'].to_dict()
    df['total_sum'] = df[f'{role}_{level}'].map(total_sum)

    # add missing values
    current_values = df[f'{role}_{level}'].to_list()
    for area in areas:
        if area not in current_values:
            df.loc[len(df)] = [area, 0]

    # add to data
    df['area'] = df[f'{role}_{level}']
    df['perc'] = (df['Gewicht_KG'] / df['total_sum']) * 100
    df = df[['area', *groupby, 'perc']]

    return df


def run():
    print(f"\nCompute reuse-recycling...")
    ALIAS = "Nederland"
    AREAS = [var.AREA, ALIAS]

    # import postcodes
    postcodes = pd.read_csv(f"{var.INPUT_DIR}/GEODATA/postcodes/{var.POSTCODES}.csv", low_memory=False)
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{var.LEVEL}"] == var.AREA]['Gemeente'].to_list()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {sorted(area_gemeenten)}')

    # import lma data
    columns = [
        'Herkomst_Postcode',
        'Herkomst_Land',
        'EuralCode',
        'VerwerkingsmethodeCode',
        'Gewicht_KG'
    ]
    path = fr"C:\Users\vasil\Downloads"
    df = pd.read_csv(f'{path}/ontvangst_{var.YEAR}_full.csv',
                     usecols=columns, dtype={'EuralCode': str},
                     low_memory=False)

    # add provincie/gemeente on herkomst & verwerker
    for role in ['Herkomst']:
        df.loc[df[f'{role}_Land'] == 'NEDERLAND', f'{role}_PC4'] = df[f'{role}_Postcode'].str[:4]
        df = add_areas(df, areas=postcodes, role=role)

    # divide between current area & others
    df.loc[
        (df[f'Herkomst_{var.LEVEL}'] != var.AREA) &
        (df[f'Herkomst_Land'] == 'NEDERLAND'),
        f'Herkomst_{var.LEVEL}'
    ] = ALIAS

    # map processes
    processes = {
        'Hergebruik': ['B01', 'B02', 'B03', 'B05', 'C04'],
        'Recycle': ['C01', 'C02', 'C03', 'D01',
                     'D02', 'D03', 'D04', 'D05', 'D06',
                     'E01', 'E02', 'E03', 'E04', 'E05',
                     'F05'],
        'Overige verwerkingsmethoden': [
            'G01', 'G02',  # storten
            'F01', 'F02',  # verbranden
            'B04', 'F03', 'F04', 'F06', 'F07'  # terugwinning
            # 'A01', 'A02'  # bewaren
        ]
    }
    for process, codes in processes.items():
        df.loc[df['VerwerkingsmethodeCode'].isin(codes), 'process'] = process

    # total waste (reuse & recycling)
    def total_waste(df):
        return df[df['EuralCode'].str[:2] != '19']
    DATA['total_reuse_recycling'] = \
        compute_lma_goal(df,
                         role='Herkomst',
                         level=var.LEVEL,
                         areas=AREAS,
                         groupby=['process'],
                         apply=total_waste,
                         year=var.YEAR)

    # construction waste (reuse & recycling)
    def construction_waste(df):
        return df[df['EuralCode'].str[:2] == '17']
    DATA['construction_reuse_recycling'] = \
        compute_lma_goal(df,
                         role='Herkomst',
                         level=var.LEVEL,
                         areas=AREAS,
                         groupby=['process'],
                         apply=construction_waste,
                         year=var.YEAR)

    # industry waste (reuse & recycling)
    def industry_waste(df):
        return df[~df['EuralCode'].str[:2].isin(['19', '20'])]
    DATA['industry_reuse_recycling'] = \
        compute_lma_goal(df,
                         role='Herkomst',
                         level=var.LEVEL,
                         areas=AREAS,
                         groupby=['process'],
                         apply=industry_waste,
                         year=var.YEAR)

    # export data to json
    results = {}
    for theme, data in DATA.items():
        # order processes in dataframe
        categories = list(processes.keys())
        data['process'] = pd.Categorical(data['process'], categories=categories, ordered=True)
        data = data.sort_values('process')

        # convert to object
        values = {}
        for idx, row in data.iterrows():
            values.setdefault(row['area'], []).append(row['perc'])
        results[theme] = {
            'values': values,
            'processes': categories,
            'year': var.YEAR,
            'unit': '%'
        }

    return results

