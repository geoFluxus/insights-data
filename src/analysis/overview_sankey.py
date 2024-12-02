from src import utils
import pandas as pd
import variables as var
import numpy as np


# VARIABLES
VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'POSTCODES': var.POSTCODES,
    'YEAR': var.YEAR,
    'COROP_FILE': var.COROP_FILE,
    'COROPS': var.COROPS,
    'OUTPUT_DIR': var.OUTPUT_DIR,
    'OVERVIEW_SANKEY_UNIT': var.UNITS['OVERVIEW']['OVERVIEW_SANKEY']
}


DATA = {}


def process_lma(polygon, ewc_classifs):
    STROMEN = {
        ('Herkomst', True, 'Verwerker', True): 'Productie van afval binnen de regio',
        ('Herkomst', True, 'Verwerker', False): 'Export van afval',
        ('Herkomst', False, 'Verwerker', True): 'Import van afval',
        # ('EerstAfnemer', True, 'Verwerker', True): 'Hergebruik van afval binnen de regio',
        # ('EerstAfnemer', True, 'Verwerker', False): 'Hergebruik van afval buiten de regio',
        # ('EerstAfnemer', False, 'Verwerker', True): 'Hergebruik van afval van elders binnen de regio'
    }

    # process LMA ontvangst & afgifte
    for typ in [
        'Ontvangst',
        #'Afgifte'
    ]:
        # import file
        print(f'\nImport {typ}...')
        path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
        filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
        df = pd.read_csv(filename, low_memory=False)

        # add areas to roles
        print('Add areas to roles...')
        source = var.ROLES[typ]['source']  # source role
        target = var.ROLES[typ]['target']  # target role
        for role in [source, target]:
            df = utils.add_areas(df,
                                 areas=polygon,
                                 role=role,
                                 admin_level=VARS['LEVEL'])

        # add classifications
        for name, classif in ewc_classifs.items():
            df = utils.add_classification(df, classif, name=name,
                                          left_on='EuralCode',
                                          right_on='ewc')

        # SANKEY
        # source in / target in
        flows, amounts = [], []
        flows.append(STROMEN[(source, True, target, True)])
        amounts.append(utils.compute_sankey_branch(df,
                                                   source=source, source_in=True,
                                                   target=target, target_in=True,
                                                   level=VARS['LEVEL'], areas=[VARS['AREA']],
                                                   unit=VARS['OVERVIEW_SANKEY_UNIT']))

        # source in / target out
        flows.append(STROMEN[(source, True, target, False)])
        amounts.append(utils.compute_sankey_branch(df,
                                                   source=source, source_in=True,
                                                   target=target, target_in=False,
                                                   level=VARS['LEVEL'], areas=[VARS['AREA']],
                                                   unit=VARS['OVERVIEW_SANKEY_UNIT']))

        # source out / target in
        flows.append(STROMEN[(source, False, target, True)])
        amounts.append(utils.compute_sankey_branch(df,
                                                   source=source, source_in=False,
                                                   target=target, target_in=True,
                                                   level=VARS['LEVEL'], areas=[VARS['AREA']],
                                                   unit=VARS['OVERVIEW_SANKEY_UNIT']))

        for flow, amount in zip(flows, amounts):
            item = DATA.setdefault("flows", {})
            key = flow.lower().replace(' ', '_')
            item[key] = {
                "values": [amount]
            }


def process_cbs():
    # stromen -> million kg
    path = f"{var.INPUT_DIR}/DATA/monitor_data/data/CBS"
    filename = f"{path}/{VARS['COROP_FILE']}.csv"

    df = pd.read_csv(filename, low_memory=False, sep=',')
    df['Gewicht_KG'] = df['Brutogew'] * 10 ** 6  # mln kg -> kg
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')

    # filter by year & COROPS
    # exclude afval and total sums
    df = df[
        (df['Jaar'] == VARS['YEAR']) &
        (df['Regionaam'].isin(VARS['COROPS'])) &
        (~df['Goederengroep_naam'].str.contains('afval', case=False, na=False)) &
        (df['Gebruiksgroep_naam'] != 'Totaal')
    ]
    stromen = [
        'Aanbod_eigen_regio',
        'Distributie',
        'Doorvoer',
        'Invoer_internationaal',
        'Invoer_nationaal',
        'Uitvoer_internationaal',
        'Uitvoer_nationaal',
        'Wederuitvoer',
        'Invoer_voor_wederuitvoer'
    ]

    # SANKEY
    for stroom in stromen:
        item = DATA.setdefault("flows", {})
        key = stroom.lower().replace(' ', '_')
        item[key] = {
            "values": [
                utils.kg_to_unit(
                    df[df['Stroom'] == stroom]['Gewicht_KG'].sum(),
                    unit=VARS['OVERVIEW_SANKEY_UNIT']
                )
            ]
        }


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/CBS"
    df = pd.read_excel(f"{path}/Huishoudelijk_Gemeenten.xlsx", sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', np.nan)
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


def process_household():
    # import postcodes
    postcodes = pd.read_csv(
        f"{VARS['INPUT_DIR']}/GEODATA/postcodes/{VARS['POSTCODES']}.csv",
        low_memory=False
    )
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{VARS['LEVEL']}"] == VARS['AREA']]['Gemeente'].to_list()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {sorted(area_gemeenten)}')

    # import household data
    print('\nImport household data...')
    household_data = import_household_data(areas=gemeenten)
    household_data = household_data.rename(columns={'Gebieden': 'Gemeente'})
    household_data = household_data[household_data['Perioden'] == int(VARS['YEAR'])]
    household_data = household_data[household_data[VARS['LEVEL']] == VARS['AREA']]

    # total household waste
    household_data['Gewicht_KG'] = household_data["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] \
                                   * household_data['Inwoners']
    household_data = household_data['Gewicht_KG'].sum()
    item = DATA.setdefault("flows", {})
    item["huishoudelijk_afval"] = {
        "values": [
            utils.kg_to_unit(
                household_data,
                unit=VARS['OVERVIEW_SANKEY_UNIT']
            )
        ]
    }


def run():
    # start analysis
    print('OVERVIEW ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['chains']:
        ewc_classifs[classif] = pd.read_csv(f"{VARS['INPUT_DIR']}/DATA/ontology/ewc_{classif}.csv",
                                            low_memory=False,
                                            sep=';')

    # process LMA data
    process_lma(polygon, ewc_classifs)

    # process CBS data
    if len(VARS['COROPS']):
        process_cbs()

    # processe household data
    process_household()

    return {
        **DATA,
        'name': var.AREA,
        'level': var.LEVEL,
        'year': var.YEAR,
        'unit': VARS['OVERVIEW_SANKEY_UNIT']
    }