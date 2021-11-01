import pandas as pd
import logging
import numpy as np
import gc
import json
import variables as var


DATA = {}

YEARS = var.YEARS


def import_household_data(areas=None, population=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    df = pd.read_excel('./data/household/Huishoudelijk_Gemeenten_Utrecht.xlsx', sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', np.nan)
    df = pd.merge(df, areas, left_on='Gebieden', right_on='Gemeente (post 2019)', how='left')
    columns.append('Provincie')
    df = df[columns]
    missing = {
        'Den Haag': 'Zuid-Holland',
        'Nuenen c.a.': 'Noord-Brabant',
    }
    for key, value in missing.items():
        df.loc[df['Gebieden'] == key, 'Provincie'] = value

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
    path = '../../../../../media/geofluxus/DATA/national/ontvangst/processed'
    print(f'Import {year} data...')
    df = pd.read_csv(f'{path}/ontvangst_{year}_full.csv', usecols=columns, low_memory=False)

    # ewc code as 6-digit
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)

    # add provincie/gemeente on herkomst & verwerker
    for role in ['Herkomst', 'Verwerker']:
        df.loc[df[f'{role}_Land'] == 'NEDERLAND', f'{role}_PC4'] = df[f'{role}_Postcode'].str[:4]
        df = add_areas(df, areas=areas, role=role)

    return df


def compute_lma_waste(df,
                      role=None,
                      level=None,
                      areas=None,
                      apply=None,
                      year=None,
                      reference=None,
                      ignore=False):
    """
    Compute LMA waste
    """
    terms = {
        'Provincie': 'province',
        'Gemeente': 'municipality'
    }
    unit = '%' if reference is not None else 'Mt'
    title = f'{terms[level]}\t{apply.__name__}\t{unit}'
    print(title)

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
    df[year] = df['Gewicht_KG'] / 10**9
    df['area'] = df[f'{role}_{level}']
    result = df[['area', year]].sort_values(by='area')
    if reference is not None:
        result[year] = (result[year] / reference[year]) * 100
    if not ignore: DATA.setdefault(title, []).append(result)

    return result


def cbs_primary_waste(input, level=None, year=None, title=None):
    print(title)

    df = input.copy()
    columns = [
        f"{level}",
        "Gewicht_KG",
        "Inwoners"
    ]
    df['Gewicht_KG'] = df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] * df['Inwoners']
    df = df[columns].groupby(f"{level}").sum().reset_index()

    # add to data
    df[year] = df['Gewicht_KG'] / 10**9
    df['area'] = df[f'{level}']
    result = df[['area', year]].sort_values(by='area')
    DATA.setdefault(title, []).append(result)

    return result


def compute_cbs_waste(input, apply=None, year=None, unit=None):
    title = f'{apply.__name__}\t{unit}'
    print(title)

    df = input.copy()

    # use apply function
    df['Gewicht_KG'] = df.apply(lambda x: apply(x), axis=1)

    # compute for the entire provincie
    df_total = df.copy()
    df = df[[
        'Gemeente',
        'Gewicht_KG',
        'Inwoners'
    ]]
    columns = [
        "Provincie",
        "Gewicht_KG",
        "Inwoners"
    ]
    df_total['Gewicht_KG'] = df_total['Gewicht_KG'] * df_total['Inwoners']
    df_total = df_total[columns].groupby("Provincie").sum().reset_index()
    df_total['Gewicht_KG'] = df_total['Gewicht_KG'] / df_total['Inwoners']

    # add to data
    df['area'] = df['Gemeente']
    df[year] = df['Gewicht_KG']
    df.reset_index(inplace=True)
    DATA.setdefault(f'municipality\t{title}', []).append(df[['area', year]])
    df_total['area'] = df_total['Provincie']
    df_total[year] = df_total['Gewicht_KG']
    df.reset_index(inplace=True)
    DATA.setdefault(f'province\t{title}', []).append(df_total[['area', year]])


if __name__ == '__main__':
    logging.basicConfig(filename="logs.txt",  # file name
                        filemode="w",  # overwrite
                        level=logging.INFO,  # lowest warning level
                        format="%(message)s"
                        )

    # import postcodes
    postcodes = pd.read_excel('./data/areas/postcodesNL.xlsx')
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente (post 2019)', 'Provincie']].drop_duplicates()
    provincie_gemeenten = gemeenten[gemeenten['Provincie'] == var.PROVINCE]['Gemeente (post 2019)'].to_list()

    # import population data
    population = pd.read_csv('../data/areas/populationNL.csv', delimiter=';')

    # import household data
    print('Import household data...\n')
    household_data = import_household_data(areas=gemeenten, population=population)
    household_data = household_data.rename(columns={'Gebieden': 'Gemeente'})

    # import data for each year
    alias = 'Other provinces'
    for year in YEARS:
        print(f'Analyse {year}...')
        LMA_FLOWS = import_lma_flows(areas=postcodes, year=year)
        CBS_FLOWS = household_data[household_data['Perioden'] == int(year)]

        # process for province & municipalities
        for level in [
            'Provincie',
            # 'Gemeente'
        ]:
            areas = [var.PROVINCE, alias] if level == 'Provincie' else provincie_gemeenten

            # copy initial dataframes
            lma_flows = LMA_FLOWS.copy()
            cbs_flows = CBS_FLOWS.copy()

            # if level == 'Provincie', divide between current area & others
            if level == 'Provincie':
                lma_flows.loc[lma_flows[f'Herkomst_{level}'] != var.PROVINCE, f'Herkomst_{level}'] = alias
                cbs_flows.loc[cbs_flows['Provincie'] != var.PROVINCE, 'Provincie'] = alias

            # if level == 'Gemeente', get only province data
            if level == 'Gemeente':
                lma_flows = lma_flows[lma_flows[f'Herkomst_Provincie'] == var.PROVINCE]
                cbs_flows = cbs_flows[cbs_flows['Provincie'] == var.PROVINCE]

            # total company primary waste (LMA) -> weight
            def total_company_primary_waste(df):
                return df[df['EuralCode'].str[:2] != '19']
            total_company_primary =\
                compute_lma_waste(lma_flows,
                                  role='Herkomst',
                                  level=level,
                                  areas=areas,
                                  apply=total_company_primary_waste,
                                  year=year)

            # total household primary waste (CBS) -> weight
            prefix = 'province' if level == 'Provincie' else 'municipality'
            title = f'{prefix}\ttotal_household_primary_waste\tMt'
            total_household_primary =\
                cbs_primary_waste(cbs_flows,
                                  level=level,
                                  year=year,
                                  title=title)

            # # total primary = company + household
            # total_primary = pd.concat([total_company_primary, total_household_primary])
            # total_primary = total_primary.groupby('area').sum().reset_index()

            # incineration waste (LMA) -> weight
            def incineration_waste(df):
                ewc = ['B04', 'F01', 'F02', 'F06', 'F07']
                return df[df['VerwerkingsmethodeCode'].isin(ewc)]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=incineration_waste,
                              year=year)

            # landfill waste -> weight
            def landfill_waste(df):
                ewc = ['G01', 'G02']
                return df[df['VerwerkingsmethodeCode'].isin(ewc)]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=landfill_waste,
                              year=year)

            # reuse of primary waste -> percentage (primary waste: company)
            def reuse_primary_waste(df):
                ewc = ['B01', 'B03', 'B05']
                return df[(df['EuralCode'].str[:2] != '19') & (df['VerwerkingsmethodeCode'].isin(ewc))]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=reuse_primary_waste,
                              year=year,
                              reference=total_company_primary)

            # recycling of primary waste -> percentage (primary waste: company)
            def recycling_primary_waste(df):
                ewc = ['C01', 'C02', 'C03', 'C04', 'D01',
                       'D02', 'D03', 'D04', 'D05', 'D06',
                       'E01', 'E02', 'E03',  'E04', 'E05',
                       'F03', 'F04' ]
                return df[(df['EuralCode'].str[:2] != '19') & (df['VerwerkingsmethodeCode'].isin(ewc))]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=recycling_primary_waste,
                              year=year,
                              reference=total_company_primary)

            # total household waste per inhabitant -> kg
            if level == 'Gemeente':
                def household_waste_per_inhabitant(df):
                    return df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"]
                compute_cbs_waste(cbs_flows,
                                  apply=household_waste_per_inhabitant,
                                  year=year,
                                  unit='kg')

                # household residual waste per inhabitant -> kg
                def residual_waste_per_inhabitant(df):
                    return df["Hoeveelheid fijn huishoudelijk restafval [Kilo's per inwoner]"] + \
                           df["Hoeveelheid grof huishoudelijk restafval [Kilo's per inwoner]"]
                compute_cbs_waste(cbs_flows,
                                  apply=residual_waste_per_inhabitant,
                                  year=year,
                                  unit='kg')

                # separation of household waste (as % of household waste) -> %
                def separation_waste_per_inhabitant(df):
                    return df['Scheidingspercentage totaal huishoudelijk afval [Percentage]'] * 100
                compute_cbs_waste(cbs_flows,
                                  apply=separation_waste_per_inhabitant,
                                  year=year,
                                  unit='%')

            # residual waste of companies, organisations & governments -> weight
            def residual_company_waste(df):
                ewc = ['200301', '200307', '200399']
                return df[df['EuralCode'].isin(ewc)]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=residual_company_waste,
                              year=year)

            # construction waste (chapter 17)
            def construction_waste(df):
                return df[df['EuralCode'].str[:2] == '17']
            construction_waste =\
                compute_lma_waste(lma_flows,
                                  role='Herkomst',
                                  level=level,
                                  areas=areas,
                                  apply=construction_waste,
                                  year=year,
                                  ignore=True)

            # reuse of construction & demolition waste -> percentage (primary waste: company construction waste)
            def reuse_construction_waste(df):
                ewc = ['B01', 'B03', 'B05']
                return df[(df['EuralCode'].str[:2] == '17') & (df['VerwerkingsmethodeCode'].isin(ewc))]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=reuse_construction_waste,
                              year=year,
                              reference=construction_waste)

            # recycling of construction & demolition waste -> percentage (primary waste: company construction waste)
            def recycling_construction_waste(df):
                ewc = ['C01', 'C02', 'C03', 'C04', 'D01',
                       'D02', 'D03', 'D04', 'D05', 'D06',
                       'E01', 'E02', 'E03', 'E04', 'E05',
                       'F03', 'F04']
                return df[(df['EuralCode'].str[:2] == '17') & (df['VerwerkingsmethodeCode'].isin(ewc))]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=recycling_construction_waste,
                              year=year,
                              reference=construction_waste)

            # food waste -> weight
            def food_waste(df):
                ewc = ['020102', '020103', '020201', '020202', '020203',
                       '020301', '020303', '020304', '020501', '020601',
                       '020701', '020702', '020704', '200301', '200399',
                       '200108', '200125', '200302']
                return df[df['EuralCode'].isin(ewc)]
            compute_lma_waste(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=food_waste,
                              year=year)

        # clean memory & collect garbage
        print('\n')
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
        results[field] = json.loads(df[columns].to_json(orient="records"))

    final = {}
    with open('../test/household.json', 'w') as outfile:
        for key, value in results.items():
            level, field, unit = key.split('\t')
            for item in value:
                name = item.pop('area')
                period, value = [],  []
                for year, amount in item.items():
                    period.append(year)
                    value.append(round(amount, 2) if type(amount) == float else 0)
                final.setdefault(field, []).append({
                    'name': name,
                    'level': level,
                    'period': period,
                    'values': {
                        'waste': {
                            'weight': {
                                'value': value,
                                'unit': unit
                            }
                        }
                    }
                })

        import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(final, outfile, indent=indent)


