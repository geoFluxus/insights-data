import utils
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
    'COROPS': var.COROPS,
    'OUTPUT_DIR': var.OUTPUT_DIR,
    'OVERVIEW_SANKEY_UNIT': var.UNITS['OVERVIEW']['OVERVIEW_SANKEY'],
    'SUPPLY_CHAINS_UNIT': var.UNITS['OVERVIEW']['SUPPLY_CHAINS']
}


DATA = {}


def process_lma():
    STROMEN = {
        ('Herkomst', True, 'Verwerker', True): 'Productie van afval binnen de regio',
        ('Herkomst', True, 'Verwerker', False): 'Export van afval',
        ('Herkomst', False, 'Verwerker', True): 'Import van afval',
        ('EerstAfnemer', True, 'Verwerker', True): 'Hergebruik van afval binnen de regio',
        ('EerstAfnemer', True, 'Verwerker', False): 'Hergebruik van afval buiten de regio',
        ('EerstAfnemer', False, 'Verwerker', True): 'Hergebruik van afval van elders binnen de regio'
    }

    # process LMA ontvangst & afgifte
    for typ in ['Ontvangst', 'Afgifte']:
        # data prefix
        prefix = f"{PREFIXES[VARS['LEVEL']]}\t{typ.lower()}meldingen"

        # import file
        print()
        print(f'Import {typ}...')
        path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
        filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
        df = pd.read_csv(filename, low_memory=False)

        # add areas to roles
        print('Add areas to roles...')
        source = ROLES[typ]['source']  # source role
        target = ROLES[typ]['target']  # target role
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

        DATA.setdefault(f"{prefix}\toverview_sankey\t{VARS['YEAR']}", []).append({
            "name": VARS['AREA'],
            "flows": flows,
            "values": {
                "weight": {
                    "value": amounts,
                    "unit": VARS['OVERVIEW_SANKEY_UNIT']
                }
            }
        })

        # SUPPLY CHAIN
        # only on primary waste (Ontvangst)
        if PREFIXES[typ] == 'primair':
            DATA[f"{prefix}\tsupply_chains\t{VARS['YEAR']}"] = \
                utils.get_classification_graphs(df,
                                                source=source,
                                                level=VARS['LEVEL'],
                                                area=VARS['AREA'],
                                                klass='chains',
                                                unit=VARS['SUPPLY_CHAINS_UNIT'])


def process_cbs():
    # stromen -> million kg
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/CBS"
    filename = f"{path}/Tabel Regionale stromen 2015-2020.csv"
    df = pd.read_csv(filename, low_memory=False)
    df['Gewicht_KG'] = df['Brutogew'] * 10 ** 6  # mln kg -> kg
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')

    # filter by year & COROPS
    # exclude chapter 24 (Afval)
    df = df[
        (df['Jaar'] == VARS['YEAR']) &
        (df['Provincienaam'].isin(VARS['COROPS'])) &
        (df['Goederengroep_nr'] != 24)
    ]
    stromen = [
        'Aanbod_eigen_regio',
        'Distributie',
        'Doorvoer',
        'Invoer_internationaal',
        'Invoer_regionaal',
        'Uitvoer_internationaal',
        'Uitvoer_regionaal'
    ]

    prefix = f"{PREFIXES[VARS['LEVEL']]}\tgoederen"

    # SANKEY
    amounts = []
    for stroom in stromen:
        amount = utils.kg_to_unit(
            df[df['Stroom'] == stroom]['Gewicht_KG'].sum(),
            unit=VARS['OVERVIEW_SANKEY_UNIT']
        )
        amounts.append(amount)
    DATA.setdefault(f"{prefix}\toverview_sankey\t{VARS['YEAR']}", []).append({
        "name": VARS['AREA'],
        "flows": stromen,
        "values": {
            "weight": {
                "value": amounts,
                "unit": VARS['OVERVIEW_SANKEY_UNIT']
            }
        }
    })

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['chains']:
        cbs_classifs[classif] = pd.read_csv(
            f"{VARS['INPUT_DIR']}/DATA/ontology/cbs_{classif}.csv", low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='Goederengroep_nr',
                                      right_on='cbs')

    # SUPPLY CHAIN
    # filter CBS input
    input_df = df[df['Stroom'].isin([
        'Aanbod_eigen_regio',
        'Invoer_internationaal',
        'Invoer_regionaal'
    ])]
    prefix = f"{PREFIXES[VARS['LEVEL']]}\tgoederen"
    DATA[f"{prefix}\tsupply_chains\t{VARS['YEAR']}"] = \
        utils.get_classification_graphs(input_df,
                                        area=VARS['AREA'],
                                        klass='chains',
                                        unit=VARS['SUPPLY_CHAINS_UNIT'])


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
    print()
    print('Import household data...\n')
    household_data = import_household_data(areas=gemeenten)
    household_data = household_data.rename(columns={'Gebieden': 'Gemeente'})
    household_data = household_data[household_data['Perioden'] == int(VARS['YEAR'])]
    household_data = household_data[household_data[VARS['LEVEL']] == VARS['AREA']]

    # total household waste
    household_data['Gewicht_KG'] = household_data["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] \
                                   * household_data['Inwoners']
    household_data = household_data['Gewicht_KG'].sum()
    prefix = f"{PREFIXES[VARS['LEVEL']]}\thuishoudelijk"
    DATA.setdefault(f"{prefix}\toverview_sankey\t{VARS['YEAR']}", []).append({
        "name": VARS['AREA'],
        "flows": ['Huishoudelijk afval'],
        "values": {
            "weight": {
                "value": [utils.kg_to_unit(
                    household_data,
                    unit=VARS['OVERVIEW_SANKEY_UNIT']
                )],
                "unit": VARS['OVERVIEW_SANKEY_UNIT']
            }
        }
    })


if __name__ == "__main__":
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES

    # start analysis
    print('OVERVIEW ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    print(polygon.crs)
    assert len(polygon) == 1

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['chains']:
        ewc_classifs[classif] = pd.read_csv(f"{VARS['INPUT_DIR']}/DATA/ontology/ewc_{classif}.csv",
                                            low_memory=False,
                                            sep=';')

    # process LMA data
    process_lma()

    # process CBS data
    if len(VARS['COROPS']):
        process_cbs()

    # processe household data
    process_household()

    # GRAPHS
    utils.export_graphs(f"{VARS['OUTPUT_DIR']}/overview.json", data=DATA)
