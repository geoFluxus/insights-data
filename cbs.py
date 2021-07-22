"""
QUESTIONS:
1) What is the weight unit in COROP flows (CBS)?
2) What does 'Nederland' mean in COROP flows (CBS)?
3) What's the difference between zero and NULL values in COROP flows (CBS)?
"""


import pandas as pd
import geopandas as gpd
import numpy as np
import json
from shapely import wkt

# IMPORT ALL FLOWS
# import MRA ontvangst (2019) -> kg
ontvangst = pd.read_csv('./data/flows/ontvangst_mra_2019.csv', low_memory=False)
# import afgifte (2019) -> kg
afgifte = pd.read_csv('./data/flows/afgifte_2019_full.csv', low_memory=False)

# process postocdes
MRA_PC4 = pd.read_csv('./data/areas/AMA_postcode.csv', low_memory=False)['AMA_postcode'].to_list()
MRA_PC4 = [str(code) for code in MRA_PC4]
ontvangst.loc[ontvangst['Herkomst_Land'] == 'NEDERLAND', 'Herkomst_PC4'] = ontvangst['Herkomst_Postcode'].str[:4]
ontvangst.loc[ontvangst['Verwerker_Land'] == 'NEDERLAND', 'Verwerker_PC4'] = ontvangst['Verwerker_Postcode'].str[:4]
afgifte.loc[afgifte['EerstAfnemer_Land'] == 'NEDERLAND', 'EerstAfnemer_PC4'] = afgifte['EerstAfnemer_Postcode'].str[:4]
afgifte.loc[ontvangst['Verwerker_Land'] == 'NEDERLAND', 'Verwerker_PC4'] = afgifte['Verwerker_Postcode'].str[:4]

# # SANKEY
# # import CBS stromen (MRA - 2019) -> million kg
# path = './data/cbs/210311 Tabel regionale stromen 2019 levering met waarde.xlsx'
# cbs_tabel = pd.read_excel(path, sheet_name='CBS_totaal')
# instroom_totaal = (cbs_tabel['Import, binnen Nederland'].sum() + cbs_tabel['Import, buiten Nederland'].sum()) / 10**3  # megatonnes
# intrastroom_totaal = (cbs_tabel['Distributie'].sum() + cbs_tabel['Aanbod'].sum()) / 10**3  # megatonnes
# uitstroom_totaal = (cbs_tabel['Export, binnen Nederland'].sum() + cbs_tabel['Export, buiten Nederland'].sum()) / 10**3  # megatonnes
#
# print(f'Instrooom: {instroom_totaal} Mtn')
# print(f'Intrastroom: {intrastroom_totaal} Mtn')
# print(f'Uitstroom: {uitstroom_totaal} Mtn')
#
#
# def compute_total(flows, source=None, target=None, source_in=True, target_in=True):
#     binnen = {
#         True: 'binnen',
#         False: 'buiten'
#     }
#     conditions = []
#     for role, is_in in zip([source, target], [source_in, target_in]):
#         condition = flows[f'{role}_PC4'].isin(MRA_PC4)
#         if not is_in: condition = ~condition
#         conditions.append(condition)
#     new_flows = flows[np.bitwise_and.reduce(conditions)]
#     new_flows = new_flows['Gewicht_KG'].sum() / 10**9  # megatonnes
#     print(f'{source} ({binnen[source_in]} MRA) -> {target} ({binnen[target_in]} MRA): {new_flows} Mtn')
#     return new_flows

# # ontvansgt: herkomst, binnen MRA -> verwerker, binnen MRA
# compute_total(ontvangst, source='Herkomst', source_in=True, target='Verwerker', target_in=True)
#
# # ontvansgt: herkomst, binnen MRA -> verwerker, buiten MRA
# compute_total(ontvangst, source='Herkomst', source_in=True, target='Verwerker', target_in=False)
#
# # ontvansgt: herkomst, buiten MRA -> verwerker, binnen MRA
# compute_total(ontvangst, source='Herkomst', source_in=False, target='Verwerker', target_in=True)
#
# # afgifte: eerstafnemer, binnen MRA -> verwerker, binnen MRA
# compute_total(afgifte, source='EerstAfnemer', source_in=True, target='Verwerker', target_in=True)
#
# # afgifte: eerstafnemer, binnen MRA -> verwerker, buiten MRA
# compute_total(afgifte, source='EerstAfnemer', source_in=True, target='Verwerker', target_in=False)
#
# # afgifte: eerstafnemer, buiten MRA -> verwerker, binnen MRA
# compute_total(afgifte, source='EerstAfnemer', source_in=False, target='Verwerker', target_in=True)


def import_cbs_flows(type=None):
    flows = pd.read_excel(f'./data/cbs/210122_COROP_{type}.xlsx')
    flows = flows[flows['Jaar'] == 2019]

    # filter areas
    flows = flows[flows['gebied'] != 'OVerige gebieden']
    flows = flows[flows['gebied'] != 'MRA']
    flows = flows[flows['gebied'] != 'Nederland']

    # edit amounts
    flows.loc[(flows['Gewicht_totaal'].isnull()) | (flows['Gewicht_totaal'] == 0), 'Gewicht_totaal'] = 1e-5

    return flows

# import CBS flows (2019)
instroom = import_cbs_flows(type='instroom')
uitstroom = import_cbs_flows(type='uitstroom')

# import areas
provincies = gpd.read_file('./data/areas/provincies.shp')
continents = gpd.read_file('./data/areas/continents.shp')
continents['name'] = continents['cont_nl']
provincies['Centroid'] = provincies.centroid
continents['Centroid'] = continents.centroid


# join flows & areas
def merge_areas(flows, areas, column=None):
    columns = list(flows.columns)
    flows = flows.merge(areas, left_on='gebied', right_on='name', how='left')
    flows[column] = flows['Centroid']
    columns.append(column)
    flows = flows[columns]
    return flows


# prepare flows
def prepare_flows(flows):
    flows = merge_areas(flows, provincies, column='Provincie')
    flows = merge_areas(flows, continents, column='Continent')
    flows.loc[flows['Provincie'].notnull(), 'Centroid'] = flows['Provincie']
    flows.loc[flows['Continent'].notnull(), 'Centroid'] = flows['Continent']
    flows = flows[flows['Centroid'].notnull()]
    columns = ['gebied', 'Centroid', 'Gewicht_totaal']
    flows['Centroid'] = flows['Centroid'].apply(lambda x: wkt.dumps(x))
    flows = flows[columns].groupby(columns[:-1]).sum().reset_index()
    return flows


instroom = prepare_flows(instroom)
uitstroom = prepare_flows(uitstroom)


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
    flows[f'{admin_level}_Centroid'] = flows['Centroid']
    flows[f'{admin_level}_Gebied'] = flows['name']
    columns.append(f'{admin_level}_Centroid')
    columns.append(f'{admin_level}_Gebied')
    flows = flows[columns]

    return flows


def get_centroid(flows, role=None):
    columns = list(flows.columns)
    flows = add_areas(flows, role=role, areas=provincies, admin_level='Provincie')
    flows = add_areas(flows, role=role, areas=continents, admin_level='Continent')
    flows[f'{role}_Gebied'] = flows['Provincie_Gebied']
    flows.loc[flows[f'{role}_Gebied'].isnull(), f'{role}_Gebied'] = flows['Continent_Gebied']
    flows = flows[flows[f'{role}_Gebied'].notnull()]
    columns.append(f'{role}_Gebied')
    flows[f'{role}_Centroid'] = flows['Provincie_Centroid']
    flows.loc[flows[f'{role}_Centroid'].isnull(), f'{role}_Centroid'] = flows['Continent_Centroid']
    flows[f'{role}_Centroid'] = flows[f'{role}_Centroid'].apply(lambda x: wkt.dumps(x))
    columns.append(f'{role}_Centroid')
    flows = flows[columns]
    return flows


ontvangst = get_centroid(ontvangst, role='Herkomst')
ontvangst = get_centroid(ontvangst, role='Verwerker')
afgifte = get_centroid(afgifte, role='EerstAfnemer')
afgifte = get_centroid(afgifte, role='Verwerker')


data = []
def get_flows(flows,
              source_role=None,
              target_role=None,
              source_in=True,
              target_in=True):
    condition = (flows[f'{source_role}_PC4'].isin(MRA_PC4)) &\
                (~flows[f'{target_role}_PC4'].isin(MRA_PC4))
    grouped = flows[condition]
    for role, is_in in zip([source_role, target_role],
                           [source_in, target_in]):
        if is_in:
            grouped[f'{role}_Gebied'] = 'MRA'
            grouped[f'{role}_Centroid'] = 'POINT(4.9, 52.366667)'
    columns = [
        f'{source_role}_Gebied',
        f'{source_role}_Centroid',
        f'{target_role}_Gebied',
        f'{target_role}_Centroid',
        'Gewicht_KG'
    ]
    grouped = grouped[columns].groupby(columns[:-1]).sum().reset_index()

    MRA = {
        'lon': 4.9,
        'lat': 52.366667,
        'name': 'MRA'
    }
    global data
    source_binnen = 'binnen' if source_in else 'buiten'
    target_binnen = 'binnen' if target_in else 'buiten'
    type = f'{source_role} ({source_binnen} MRA) -> {target_role} ({target_binnen} MRA)'

    for idx, row in grouped.iterrows():
        flow = {
            'type': type,
            'amount': row['Gewicht_KG']
        }

        for item, role, is_in in zip(['source', 'target'],
                                     [source_role, target_role],
                                     [source_in, target_in]):
            if is_in:
                flow[item] = {
                    'lon': 4.9,
                    'lat': 52.366667,
                    'name': 'MRA'
                }
            else:
                geometry = wkt.loads(row[f'{role}_Centroid'])
                flow[item] = {
                    'lon': geometry.x,
                    'lat': geometry.y,
                    'name': row[f'{role}_Gebied']
                }
        data.append(flow)


# ontvansgt: herkomst, binnen MRA -> verwerker, buiten MRA
get_flows(ontvangst, source_role='Herkomst', source_in=True, target_role='Verwerker', target_in=False)
# ontvansgt: herkomst, binnen MRA -> verwerker, binnen MRA
get_flows(ontvangst, source_role='Herkomst', source_in=True, target_role='Verwerker', target_in=True)
# ontvansgt: herkomst, buiten MRA -> verwerker, binnen MRA
get_flows(ontvangst, source_role='Herkomst', source_in=False, target_role='Verwerker', target_in=True)

# afgifte: eerstafnemer, binnen MRA -> verwerker, buiten MRA
get_flows(afgifte, source_role='EerstAfnemer', source_in=True, target_role='Verwerker', target_in=False)
# afgifte: eerstafnemer, binnen MRA -> verwerker, binnen MRA
get_flows(afgifte, source_role='EerstAfnemer', source_in=True, target_role='Verwerker', target_in=True)
# afgifte: eerstafnemer, buiten MRA -> verwerker, binnen MRA
get_flows(afgifte, source_role='EerstAfnemer', source_in=False, target_role='Verwerker', target_in=True)

# cbs
for idx, row in instroom.iterrows():
    geometry = wkt.loads(row['Centroid'])
    flow = {
        'type': f'Instroom',
        'amount': row['Gewicht_totaal'],
        'source': {
            'lon': geometry.x,
            'lat': geometry.y,
            'name': row['gebied']
        },
        'target': {
            'lon': 4.9,
            'lat': 52.366667,
            'name': 'MRA'
        }
    }
    data.append(flow)
for idx, row in uitstroom.iterrows():
    geometry = wkt.loads(row['Centroid'])
    flow = {
        'type': f'Uitstroom',
        'amount': row['Gewicht_totaal'],
        'target': {
            'lon': geometry.x,
            'lat': geometry.y,
            'name': row['gebied']
        },
        'source': {
            'lon': 4.9,
            'lat': 52.366667,
            'name': 'MRA'
        }
    }
    data.append(flow)

with open('test/cbs.json', 'w') as outfile:
    json.dump(data, outfile)