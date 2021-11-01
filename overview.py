import utils
import pandas as pd
import variables as var


# INPUTS
PROVINCE = 'Utrecht'
YEAR = 2019
COROPS = [
    'Utrecht'
]

DATA = {}


def process_lma():
    # process LMA ontvangst & afgifte
    for typ in ['Ontvangst', 'Afgifte']:
        # data prefix
        prefix = f"{PREFIXES['Provincie']}\t{PREFIXES[typ]} waste"

        # import file
        print()
        print(f'Import {typ}...')
        path = f'../../../../../media/geofluxus/DATA/national/{PROVINCE.lower()}/processed'
        filename = f'{path}/{typ.lower()}_{PROVINCE.lower()}_{YEAR}.csv'
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
        utils.compute_sankey_branch(df,
                                    source=source, source_in=True,
                                    target=target, target_in=True,
                                    level='Provincie', areas=[PROVINCE])

        # source in / target out
        utils.compute_sankey_branch(df,
                                    source=source, source_in=True,
                                    target=target, target_in=False,
                                    level='Provincie', areas=[PROVINCE])

        # source out / target in
        utils.compute_sankey_branch(df,
                                    source=source, source_in=False,
                                    target=target, target_in=True,
                                    level='Provincie', areas=[PROVINCE])

        # SUPPLY CHAIN
        # only on primary waste (Ontvangst)
        if PREFIXES[typ] == 'primary':
            DATA[f'{prefix}\tsupply_chains\t{YEAR}'] = \
                utils.get_classification_graphs(df,
                                                source=source,
                                                level='Provincie',
                                                area=PROVINCE,
                                                klass='chains')


def process_cbs():
    # stromen -> million kg
    path = './data/cbs/Tabel Regionale stromen 2015-2019.csv'
    df = pd.read_csv(path, low_memory=False, sep=';')

    # filter by year & COROPS
    df = df[(df['Jaar'] == YEAR) & (df['COROP_naam'].isin(COROPS))]
    stromen = [
        'Aanbod_eigen_regio',
        'Distributie',
        'Doorvoer',
        'Invoer_internationaal',
        'Invoer_regionaal',
        'Uitvoer_internationaal',
        'Uitvoer_regionaal'
    ]

    # SANKEY
    for stroom in stromen:
        print(f"{stroom}: {df[df['Stroom'] == stroom]['Brutogew'].sum() / 10**3}")

    # import cbs classifications
    cbs_classifs = {}
    for classif in ['chains']:
        cbs_classifs[classif] = pd.read_csv(f'./data/materials/cbs_{classif}.csv', low_memory=False, sep=';')

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
    prefix = f"{PREFIXES['Provincie']}\tmaterial"
    DATA[f'{prefix}\tsupply_chains\t{YEAR}'] = \
        utils.get_classification_graphs(input_df,
                                        area=PROVINCE,
                                        klass='chains')


if __name__ == "__main__":
    ROLES = var.ROLES
    PREFIXES = var.PREFIXES

    # import province polygon
    polygon = utils.import_areas(level='provincies')
    polygon = polygon[polygon['name'] == PROVINCE]
    assert len(polygon) == 1

    # import ewc classifications
    ewc_classifs = {}
    for classif in ['chains']:
        ewc_classifs[classif] = pd.read_csv(f'./data/materials/ewc_{classif}.csv',
                                            low_memory=False,
                                            sep=';')

    # start analysis
    print(f'YEAR: {YEAR}')

    # process LMA data
    process_lma()

    # process CBS data
    process_cbs()

    # GRAPHS
    utils.export_graphs('./test/overview.json', data=DATA)


