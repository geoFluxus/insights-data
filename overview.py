import utils
import pandas as pd
import variables as var


# INPUTS
DIRECTORY = var.DIRECTORY
AREA = var.AREA
LEVEL = var.LEVEL
YEAR = var.YEAR
COROPS = var.COROPS


DATA = {}


def process_lma():
    STROMEN = {
        ('Herkomst', True, 'Verwerker', True): 'Productie van afval binnen de provincie',
        ('Herkomst', True, 'Verwerker', False): 'Export van afval',
        ('Herkomst', False, 'Verwerker', True): 'Import van afval',
        ('EerstAfnemer', True, 'Verwerker', True): 'Hergebruik van afval binnen de provincie',
        ('EerstAfnemer', True, 'Verwerker', False): 'Hergebruik van afval buiten de provincie',
        ('EerstAfnemer', False, 'Verwerker', True): 'Hergebruik van afval van elders binnen de provincie'
    }

    # process LMA ontvangst & afgifte
    for typ in ['Ontvangst', 'Afgifte']:
        # data prefix
        prefix = f"{PREFIXES['Provincie']}\t{typ.lower()}meldingen"

        # import file
        print()
        print(f'Import {typ}...')
        path = f'{DIRECTORY}/{AREA}/LMA/processed'
        filename = f'{path}/{typ.lower()}_{AREA.lower()}_{YEAR}.csv'
        df = pd.read_csv(filename, low_memory=False)

        # add areas to roles
        print('Add areas to roles...')
        source = ROLES[typ]['source']  # source role
        target = ROLES[typ]['target']  # target role
        for role in [source, target]:
            df = utils.add_areas(df,
                                 areas=polygon,
                                 role=role,
                                 admin_level='Provincie')

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
                                                   level='Provincie', areas=[AREA]))

        # source in / target out
        flows.append(STROMEN[(source, True, target, False)])
        amounts.append(utils.compute_sankey_branch(df,
                                                   source=source, source_in=True,
                                                   target=target, target_in=False,
                                                   level='Provincie', areas=[AREA]))

        # source out / target in
        flows.append(STROMEN[(source, False, target, True)])
        amounts.append(utils.compute_sankey_branch(df,
                                                   source=source, source_in=False,
                                                   target=target, target_in=True,
                                                   level='Provincie', areas=[AREA]))

        DATA.setdefault(f'{prefix}\toverview_sankey\t{YEAR}', []).append({
            "name": AREA,
            "flows": flows,
            "values": {
                "weight": {
                    "value": amounts,
                    "unit": 'Mt'
                }
            }
        })

        # SUPPLY CHAIN
        # only on primary waste (Ontvangst)
        if PREFIXES[typ] == 'primair':
            DATA[f'{prefix}\tsupply_chains\t{YEAR}'] = \
                utils.get_classification_graphs(df,
                                                source=source,
                                                level='Provincie',
                                                area=AREA,
                                                klass='chains',
                                                unit='kt')


def process_cbs():
    # stromen -> million kg
    path = f'{DIRECTORY}/{AREA}/CBS/Tabel Regionale stromen 2015-2019.csv'
    df = pd.read_csv(path, low_memory=False, sep=';')

    # filter by year & COROPS
    # exclude chapter 24 (Afval)
    df = df[
        (df['Jaar'] == YEAR) &
        (df['COROP_naam'].isin(COROPS)) &
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

    prefix = f"{PREFIXES['Provincie']}\tgoederen"

    # SANKEY
    amounts = []
    for stroom in stromen:
        amount = round(df[df['Stroom'] == stroom]['Brutogew'].sum() / 10**3, 2)
        amounts.append(amount)
        print(f"{stroom}: {amount} Mt")
    DATA.setdefault(f'{prefix}\toverview_sankey\t{YEAR}', []).append({
        "name": AREA,
        "flows": stromen,
        "values": {
            "weight": {
                "value": amounts,
                "unit": 'Mt'
            }
        }
    })

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['chains']:
        cbs_classifs[classif] = pd.read_csv(
            f'{DIRECTORY}/DATA/ontology/cbs_{classif}.csv', low_memory=False, sep=';')

    # add classifications
    for name, classif in cbs_classifs.items():
        df = utils.add_classification(df, classif, name=name,
                                      left_on='Goederengroep_nr',
                                      right_on='cbs')

    # SUPPLY CHAIN
    # filter CBS input
    df['Gewicht_KG'] = df['Brutogew'] * 10 ** 6
    df['Gewicht_KG'] = df['Gewicht_KG'].astype('int64')
    input_df = df[df['Stroom'].isin([
        'Aanbod_eigen_regio',
        'Invoer_internationaal',
        'Invoer_regionaal'
    ])]
    DATA[f'{prefix}\tsupply_chains\t{YEAR}'] = \
        utils.get_classification_graphs(input_df,
                                        area=AREA,
                                        klass='chains',
                                        unit='kt')


if __name__ == "__main__":
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES

    # import province polygon
    polygon = utils.import_areas(level=LEVEL)
    polygon = polygon[polygon['name'] == AREA]
    assert len(polygon) == 1

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['chains']:
        ewc_classifs[classif] = pd.read_csv(f'{DIRECTORY}/DATA/ontology/ewc_{classif}.csv',
                                            low_memory=False,
                                            sep=';')

    # start analysis
    print(f'YEAR: {YEAR}')

    # process LMA data
    process_lma()

    # process CBS data
    process_cbs()

    # GRAPHS
    utils.export_graphs('./json/overview.json', data=DATA)
