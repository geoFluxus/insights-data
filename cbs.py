import pandas as pd
import geopandas as gpd
import numpy as np

# # IMPORT ALL FLOWS
# # import MRA ontvangst (2019) -> kg
# ontvangst = pd.read_csv('./data/flows/ontvangst_mra_2019.csv', low_memory=False)
# # import afgifte (2019) -> kg
# afgifte = pd.read_csv('./data/flows/afgifte_2019_full.csv', low_memory=False)
#
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
#
#
# # process postocdes
# MRA_PC4 = pd.read_csv('./data/areas/AMA_postcode.csv', low_memory=False)['AMA_postcode'].to_list()
# MRA_PC4 = [str(code) for code in MRA_PC4]
# ontvangst['Herkomst_PC4'] = ontvangst['Herkomst_Postcode'].str[:4]
# ontvangst['Verwerker_PC4'] = ontvangst['Verwerker_Postcode'].str[:4]
# afgifte['EerstAfnemer_PC4'] = afgifte['EerstAfnemer_Postcode'].str[:4]
# afgifte['Verwerker_PC4'] = afgifte['Verwerker_Postcode'].str[:4]
#
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


instroom = merge_areas(instroom, provincies, column='Provincie')
instroom = merge_areas(instroom, continents, column='Continent')
instroom.loc[instroom['Provincie'].notnull(), 'Centroid'] = instroom['Provincie']
instroom.loc[instroom['Continent'].notnull(), 'Centroid'] = instroom['Continent']
uitstroom = merge_areas(uitstroom, provincies, column='Provincie')
uitstroom = merge_areas(uitstroom, continents, column='Continent')
uitstroom.loc[uitstroom['Provincie'].notnull(), 'Centroid'] = uitstroom['Provincie']
uitstroom.loc[instroom['Continent'].notnull(), 'Centroid'] = uitstroom['Continent']




