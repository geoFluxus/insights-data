import pandas as pd
import geopandas as gpd
import numpy as np
import variables as var
import itertools
import _make_iterencode
import json


INPUTS = [
    {
        'year': 2019,
        'period': 'year', # year
    }
]

# COROPs in province
COROPS = [
    'Utrecht'
]

ROLES = {
    'Ontvangst': {
        'source': 'Herkomst',
        'target': 'Verwerker',
        'activity': 'Ontdoener'
    },
    'Afgifte': {
        'source': 'EerstAfnemer',
        'target': 'Verwerker',
        'activity': 'EerstAfnemer'
    }
}

AREAS = {}

DATA = {}


def import_areas():
    # municipalities
    gemeenten = gpd.read_file('data/areas/gemeenten.shp')
    gemeenten['centroid'] = gemeenten['geometry'].centroid
    # gemeenten['centroid'] = gemeenten['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    AREAS['Gemeente'] = gemeenten

    # provinces
    provincies = gpd.read_file('data/areas/provincies.shp')
    provincies['centroid'] = provincies['geometry'].centroid
    # provincies['centroid'] = provincies['geometry'].to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    AREAS['Provincie'] = provincies

    # list of province municipalities
    provincie_gemeenten = gemeenten[gemeenten['parent'] == var.PROVINCE]['name'].to_list()

    return provincie_gemeenten


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


def compute_sankey_branch(flows,
                          source=None, source_in=True,
                          target=None, target_in=True,
                          level=None, areas=[]):
    binnen = {
        True: 'binnen',
        False: 'buiten'
    }
    conditions = []
    for area in areas:
        for role, is_in in zip([source, target], [source_in, target_in]):
            condition = flows[f'{role}_{level}'].isin([area])
            if not is_in: condition = ~condition
            conditions.append(condition)
        new_flows = flows[np.bitwise_and.reduce(conditions)]
        new_flows = new_flows['Gewicht_KG'].sum() / 10**9  # megatonnes
        print(f'{source} ({binnen[source_in]} {area}) -> {target} ({binnen[target_in]} {area}): {new_flows} Mtn')


def get_classification_graphs(df, period=None, source=None,
                              level=None, areas=None, klass=None):
    # filter source in areas
    flows = df[df[f'{source}_{level}'].isin(areas)]
    flows = flows.rename(columns={source: 'source'})

    # filter by period
    if period: flows = flows[flows['Periode'] == period]

    # groupby: source, materials
    groupby = [
        f'{source}_{level}',
        klass,
        'Gewicht_KG'
    ]
    groups = flows[groupby].groupby(groupby[:-1]).sum().reset_index()

    results = []
    for area in areas:
        select = groups[groups[f'{source}_{level}'] == area]

        collection = {}
        for idx, row in select.iterrows():
            # classifs = row[klass].split('&')
            # for classif in classifs:
            collection[row[klass]] = collection.get(row[klass], 0) + row['Gewicht_KG']

        classifs, values = [],  []
        for classif, value in collection.items():
            classifs.append(classif)
            values.append(round(value / 10**9, 2))  # kg -> Mt

        results.append({
            "name": area,
            klass: classifs,
            "values": {
                "weight": {
                    "value": values,
                    "unit": "Mt"
                }
            }
        })

    return results


def add_classification(df, classif, name=None,
                       left_on=None, right_on=None):
    classif[right_on] = classif[right_on].astype(str).str.zfill(6)
    df[left_on] = df[left_on].astype(str).str.zfill(6)
    df = pd.merge(df, classif, how='left', left_on=left_on, right_on=right_on)
    df.loc[df[name].isnull(), name] = 'Unknown'
    return df


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    provincie_gemeenten = import_areas()
    provincie = var.PROVINCE

    # import ewc classifications
    classifs = {}
    for classif in ['chains']:
        classifs[classif] = pd.read_csv(f'./data/materials/ewc_{classif}.csv', low_memory=False, sep=';')

    # LMA DATA
    for input in INPUTS:
        year = input['year']
        period = input['period']
        periods = input.get('periods', [None])

        print(f'YEAR: {year}')

        for typ in ['Ontvangst', 'Afgifte']:
            # import file
            print('')
            print(f'Import {typ}....')
            path = f'../../../../../media/geofluxus/DATA/national/{var.PROVINCE.lower()}/processed'
            filename = f'{path}/{typ.lower()}_{var.PROVINCE.lower()}_{year}.csv'
            df = pd.read_csv(filename, low_memory=False)

            # add areas to roles
            print('Add areas to roles...')
            source = ROLES[typ]['source']  # source role
            target = ROLES[typ]['target']  # target role
            for role, level in itertools.product([source, target], ['Provincie']):
                areas = AREAS[level]
                df = add_areas(df, areas=areas, role=role, admin_level=level)

            # add classifications
            for name, classif in classifs.items():
                df = add_classification(df, classif, name=name,
                                        left_on='EuralCode',
                                        right_on='ewc')

            prefixes = {
                'Provincie': 'province',
                'Gemeente': 'municipality',
                'Ontvangst': 'primary',
                'Afgifte': 'secondary',
            }

            # analyse on provincial & municipal level
            print('Analyse...')
            for level in [
                'Provincie',
            ]:
                areas = [provincie] if level == 'Provincie' else provincie_gemeenten

                prefix = f'{prefixes[level]}\t{prefixes[typ]} waste'

                for p in periods:
                    suffix = f'{year}'
                    if p: suffix = f'{suffix}-{period[0].upper()}{p}'

                    # # SANKEY
                    # # source in / target in
                    # compute_sankey_branch(df,
                    #                       source=source, source_in=True,
                    #                       target=target, target_in=True,
                    #                       level=level, areas=areas)
                    #
                    # # source in / target out
                    # compute_sankey_branch(df,
                    #                       source=source, source_in=True,
                    #                       target=target, target_in=False,
                    #                       level=level, areas=areas)
                    #
                    # # source out / target in
                    # compute_sankey_branch(df,
                    #                       source=source, source_in=False,
                    #                       target=target, target_in=True,
                    #                       level=level, areas=areas)

                    # SUPPLY CHAIN
                    # only on province level & primary waste
                    if prefixes[level] == 'province' and prefixes[typ] == 'primary':
                        DATA[f'{prefix}\tsupply_chains\t{suffix}'] = \
                            get_classification_graphs(df,
                                                      period=p,
                                                      source=source,
                                                      level=level, areas=areas,
                                                      klass='chains')

        # CBS DATA
        # stromen -> million kg
        path = './data/cbs/Tabel Regionale stromen 2015-2019.csv'
        df = pd.read_csv(path, low_memory=False, sep=';')
        # filter by year & COROPS
        df = df[(df['Jaar'] == year) & (df['COROP_naam'].isin(COROPS))]
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
        # for stroom in stromen:
        #     print(f"{stroom}: {df[df['Stroom'] == stroom]['Brutogew'].sum() / 10**3}")

        # SUPPLY CHAIN
        # import cbs classifications
        classifs = {}
        for classif in ['chains']:
            classifs[classif] = pd.read_csv(f'./data/materials/cbs_{classif}.csv', low_memory=False, sep=';')
        # add classifications
        for name, classif in classifs.items():
            df = add_classification(df, classif, name=name,
                                    left_on='Goederengroep_nr',
                                    right_on='cbs')

        # groupby: source, materials
        groupby = [
            'chains',
            'Brutogew'
        ]
        groups = df[groupby].groupby(groupby[:-1]).sum().reset_index()

        results = []

        collection = {}
        for idx, row in groups.iterrows():
            collection[row['chains']] = collection.get(row['chains'], 0) + row['Brutogew']

        classifs, values = [], []
        for classif, value in collection.items():
            classifs.append(classif)
            values.append(round(value / 10**3, 2))  # ml kg -> Mt

        results.append({
            "name": var.PROVINCE,
            "chains": classifs,
            "values": {
                "weight": {
                    "value": values,
                    "unit": "Mt"
                }
            }
        })
        prefix = 'province\tmaterial'
        DATA[f'{prefix}\tsupply_chains\t{suffix}'] = results

        # GRAPHS
        with open('test/overview.json', 'w') as outfile:
            # preprocess
            results = {}
            for key, items in DATA.items():
                level, type, field, period = key.split('\t')
                for item in items:
                    item['level'] = level
                    item['period'] = period
                    item['type'] = type
                    results.setdefault(field, []).append(item)

            json.encoder._make_iterencode = _make_iterencode._make_iterencode
            indent = (2, None)
            json.dump(results, outfile, indent=indent)