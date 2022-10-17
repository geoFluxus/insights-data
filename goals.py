import pandas as pd
import numpy as np
import gc
import json
import variables as var
import utils


# VARIABLES
VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'YEARS': var.GOALS_YEARS,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'POSTCODES': var.POSTCODES,
    'OUTPUT_DIR': var.OUTPUT_DIR,
    'NATIONAL_GOALS_UNIT': var.UNITS['GOALS']['NATIONAL_GOALS']
}


DATA = {}


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/CBS"
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
    path = f"{VARS['INPUT_DIR']}/DATA/LMA/ontvangst/processed"
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
                      reference=None,
                      ignore=False):
    """
    Compute LMA waste
    """
    unit = '%' if reference is not None else VARS['NATIONAL_GOALS_UNIT']
    title = f'{PREFIXES[level]}\t{apply.__name__}\t{unit}'
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
    df[year] = utils.kg_to_unit(df['Gewicht_KG'], unit=VARS['NATIONAL_GOALS_UNIT'])
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
    df[year] = utils.kg_to_unit(df['Gewicht_KG'], unit=VARS['NATIONAL_GOALS_UNIT'])
    df['area'] = df[f'{level}']
    result = df[['area', year]].sort_values(by='area')
    DATA.setdefault(title, []).append(result)

    return result


def compute_cbs_goal(input, apply=None, year=None, unit=None):
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
        f"{VARS['LEVEL']}",
        "Gewicht_KG",
        "Inwoners"
    ]
    df_total['Gewicht_KG'] = df_total['Gewicht_KG'] * df_total['Inwoners']
    df_total = df_total[columns].groupby(f"{VARS['LEVEL']}").sum().reset_index()
    df_total['Gewicht_KG'] = df_total['Gewicht_KG'] / df_total['Inwoners']

    # add to data
    df['area'] = df['Gemeente']
    df[year] = df['Gewicht_KG']
    df.reset_index(inplace=True)
    # DATA.setdefault(f'municipality\t{title}', []).append(df[['area', year]])
    df_total['area'] = df_total[f"{VARS['LEVEL']}"]
    df_total[year] = df_total['Gewicht_KG']
    df.reset_index(inplace=True)
    DATA.setdefault(f"{VARS['LEVEL'].lower()}\t{title}", []).append(df_total[['area', year]])


if __name__ == '__main__':
    PREFIXES = var.PREFIXES
    TREATMENT_METHODS = var.TREATMENT_METHODS

    # start analysis
    print('GOALS ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import postcodes
    postcodes = pd.read_csv(f"{VARS['INPUT_DIR']}/GEODATA/postcodes/{VARS['POSTCODES']}.csv", low_memory=False)
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{VARS['LEVEL']}"] == VARS['AREA']]['Gemeente'].to_list()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {sorted(area_gemeenten)}')

    # import household data
    print()
    print('Import household data...\n')
    household_data = import_household_data(areas=gemeenten)
    household_data = household_data.rename(columns={'Gebieden': 'Gemeente'})

    # import data for each year
    ALIAS = f"Andere {'provincies' if VARS['LEVEL'] == 'Provincie' else 'gemeenten'}"
    for year in VARS['YEARS']:
        print(f'Analyse {year}...')
        LMA_FLOWS = import_lma_flows(areas=postcodes, year=year)
        CBS_FLOWS = household_data[household_data['Perioden'] == int(year)]

        areas = [VARS['AREA'], ALIAS]
        level = VARS['LEVEL']

        # copy initial dataframes
        lma_flows = LMA_FLOWS.copy()
        cbs_flows = CBS_FLOWS.copy()

        # divide between current area & others
        lma_flows.loc[
            (lma_flows[f'Herkomst_{level}'] != VARS['AREA']) &
            (lma_flows[f'Herkomst_Land'] == 'NEDERLAND'),
            f'Herkomst_{level}'
        ] = ALIAS
        cbs_flows.loc[cbs_flows[f"{VARS['LEVEL']}"] != VARS['AREA'], f"{VARS['LEVEL']}"] = ALIAS

        # total company primary waste (LMA) -> weight
        def total_company_primary_waste(df):
            return df[df['EuralCode'].str[:2] != '19']
        total_company_primary =\
            compute_lma_goal(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=total_company_primary_waste,
                              year=year)

        # total household primary waste (CBS) -> weight
        prefix = PREFIXES[level]
        title = f"{prefix}\ttotal_household_primary_waste\t{VARS['NATIONAL_GOALS_UNIT']}"
        total_household_primary =\
            cbs_primary_waste(cbs_flows,
                              level=level,
                              year=year,
                              title=title)

        # # total primary = company + household
        # total_primary = pd.concat([total_company_primary, total_household_primary])
        # total_primary = total_primary.groupby('area').sum().reset_index()z

        # incineration waste (LMA) -> weight
        def incineration_waste(df):
            process_codes = TREATMENT_METHODS['verbranden']
            return df[df['VerwerkingsmethodeCode'].isin(process_codes)]
        compute_lma_goal(lma_flows,
                          role='Herkomst',
                          level=level,
                          areas=areas,
                          apply=incineration_waste,
                          year=year)

        # landfill waste -> weight
        def landfill_waste(df):
            process_codes = TREATMENT_METHODS['storten']
            return df[df['VerwerkingsmethodeCode'].isin(process_codes)]
        compute_lma_goal(lma_flows,
                          role='Herkomst',
                          level=level,
                          areas=areas,
                          apply=landfill_waste,
                          year=year)

        # reuse of primary waste -> percentage (primary waste: company)
        def reuse_primary_waste(df):
            process_codes = TREATMENT_METHODS['hergebruiken']
            return df[(df['EuralCode'].str[:2] != '19') &
                      (df['VerwerkingsmethodeCode'].isin(process_codes))]
        compute_lma_goal(lma_flows,
                          role='Herkomst',
                          level=level,
                          areas=areas,
                          apply=reuse_primary_waste,
                          year=year,
                          reference=total_company_primary)

        # recycling of primary waste -> percentage (primary waste: company)
        def recycling_primary_waste(df):
            process_codes = TREATMENT_METHODS['recyclen']
            return df[(df['EuralCode'].str[:2] != '19') &
                      (df['VerwerkingsmethodeCode'].isin(process_codes))]
        compute_lma_goal(lma_flows,
                          role='Herkomst',
                          level=level,
                          areas=areas,
                          apply=recycling_primary_waste,
                          year=year,
                          reference=total_company_primary)

        # retrieve cbs flows for province municipalities
        # (to be used for per_inhabitant goals)
        gemeenten_cbs_flows = cbs_flows.copy()
        gemeenten_cbs_flows = gemeenten_cbs_flows[
            gemeenten_cbs_flows[f"{VARS['LEVEL']}"] == VARS['AREA']
        ]

        # total household waste per inhabitant -> kg
        def household_waste_per_inhabitant(df):
            return df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"]
        compute_cbs_goal(gemeenten_cbs_flows,
                          apply=household_waste_per_inhabitant,
                          year=year,
                          unit='kg')

        # household residual waste per inhabitant -> kg
        def residual_waste_per_inhabitant(df):
            return df["Hoeveelheid fijn huishoudelijk restafval [Kilo's per inwoner]"] + \
                   df["Hoeveelheid grof huishoudelijk restafval [Kilo's per inwoner]"]
        compute_cbs_goal(gemeenten_cbs_flows,
                          apply=residual_waste_per_inhabitant,
                          year=year,
                          unit='kg')

        # separation of household waste (as % of household waste) -> %
        def separation_waste_per_inhabitant(df):
            return df['Scheidingspercentage totaal huishoudelijk afval [Percentage]'] * 100
        compute_cbs_goal(gemeenten_cbs_flows,
                          apply=separation_waste_per_inhabitant,
                          year=year,
                          unit='%')

        # residual waste of companies, organisations & governments -> weight
        def residual_company_waste(df):
            ewc = ['200301', '200307', '200399']
            return df[df['EuralCode'].isin(ewc)]
        compute_lma_goal(lma_flows,
                          role='Herkomst',
                          level=level,
                          areas=areas,
                          apply=residual_company_waste,
                          year=year)

        # construction waste (chapter 17)
        def construction_waste(df):
            return df[df['EuralCode'].str[:2] == '17']
        construction_waste =\
            compute_lma_goal(lma_flows,
                              role='Herkomst',
                              level=level,
                              areas=areas,
                              apply=construction_waste,
                              year=year,
                              ignore=True)

        # reuse of construction & demolition waste -> percentage (primary waste: company construction waste)
        def reuse_construction_waste(df):
            process_codes = TREATMENT_METHODS['hergebruiken']
            return df[(df['EuralCode'].str[:2] == '17') &
                      (df['VerwerkingsmethodeCode'].isin(process_codes))]
        compute_lma_goal(lma_flows,
                          role='Herkomst',
                          level=level,
                          areas=areas,
                          apply=reuse_construction_waste,
                          year=year,
                          reference=construction_waste)

        # recycling of construction & demolition waste -> percentage (primary waste: company construction waste)
        def recycling_construction_waste(df):
            process_codes = TREATMENT_METHODS['recyclen']
            return df[(df['EuralCode'].str[:2] == '17') &
                      (df['VerwerkingsmethodeCode'].isin(process_codes))]
        compute_lma_goal(lma_flows,
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
        compute_lma_goal(lma_flows,
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
    with open(f"{VARS['OUTPUT_DIR']}/household.json", 'w') as outfile:
        for key, value in results.items():
            level, field, unit = key.split('\t')
            for item in value:
                name = item.pop('area')
                period, value = [],  []
                for year, amount in item.items():
                    period.append(year)
                    value.append(amount if type(amount) == float else 0)
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



