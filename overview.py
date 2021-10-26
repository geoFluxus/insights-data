import pandas as pd
import geopandas as gpd
import numpy as np
import variables as var
import itertools


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


if __name__ == "__main__":
    # import areas
    print('Import areas...')
    provincie_gemeenten = import_areas()
    provincie = var.PROVINCE

    for input in INPUTS:
        year = input['year']
        period = input['period']
        periods = input.get('periods', [None])

        print(f'YEAR: {year}')

        # recover LMA data
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

                # SANKEY
                # source in / target in
                compute_sankey_branch(df,
                                      source=source, source_in=True,
                                      target=target, target_in=True,
                                      level=level, areas=areas)

                # source in / target out
                compute_sankey_branch(df,
                                      source=source, source_in=True,
                                      target=target, target_in=False,
                                      level=level, areas=areas)

                # source out / target in
                compute_sankey_branch(df,
                                      source=source, source_in=False,
                                      target=target, target_in=True,
                                      level=level, areas=areas)

        # # recover CBS data
        # SANKEY
        # # stromen -> million kg
        # path = './data/cbs/Tabel Regionale stromen 2015-2019.csv'
        # cbs = pd.read_csv(path, low_memory=False, sep=';')
        #
        # # filter by year & COROPS
        # cbs = cbs[(cbs['Jaar'] == year) & (cbs['COROP_naam'].isin(COROPS))]
        # stromen = [
        #     'Aanbod_eigen_regio',
        #     'Distributie',
        #     'Doorvoer',
        #     'Invoer_internationaal',
        #     'Invoer_regionaal',
        #     'Uitvoer_internationaal',
        #     'Uitvoer_regionaal'
        # ]
        # # instroom_totaal = (cbs_tabel['Import, binnen Nederland'].sum() + cbs_tabel['Import, buiten Nederland'].sum()) / 10**3  # megatonnes
        # # intrastroom_totaal = (cbs_tabel['Distributie'].sum() + cbs_tabel['Aanbod'].sum()) / 10**3  # megatonnes
        # # uitstroom_totaal = (cbs_tabel['Export, binnen Nederland'].sum() + cbs_tabel['Export, buiten Nederland'].sum()) / 10**3  # megatonnes
        # for stroom in stromen:
        #     print(f"{stroom}: {cbs[cbs['Stroom'] == stroom]['Brutogew'].sum() / 10**3}")