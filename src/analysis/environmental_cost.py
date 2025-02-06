import pandas as pd
import variables as var
import numpy as np


FILEPATH = f"{var.INPUT_DIR}/DATA/monitor_data/data"
DATA = {}


def calculate_impacts(data_file='', impact_file='', group_relation_file=''):
    data = pd.read_excel(data_file)
    if 'Unnamed: 0' in data.columns:
        data.drop(columns='Unnamed: 0', inplace=True)

    impacts = pd.read_excel(impact_file).drop(columns='Unnamed: 0')
    groups = pd.read_excel(group_relation_file, sheet_name='CBS67').drop(columns='Unnamed: 0')
    ta = pd.read_excel(group_relation_file, sheet_name='CBS_code_merger')

    data = pd.merge(data, groups, left_on='Goederengroep', right_on='Goederengroep_naam', how='left')
    data = pd.merge(data, impacts, how='left', left_on='Goederengroep_nr', right_on='Goederengroep_code')
    data = pd.merge(data, ta[['Goederengroep_nr', 'TA']], how='left', on='Goederengroep_nr')
    data['CO2 emissions (kg CO2e/kg)'] = data['CO2 emissions (kg CO2e/kg)'].astype(float)
    data['Impact category (Euro/kg)'] = data['Impact category (Euro/kg)'].astype(float)

    data['CO2 emissions total (kt)'] = data['DMI'] * data['CO2 emissions (kg CO2e/kg)']  #In kt
    data['MKI total (mln euro)'] = data['DMI'] * data['Impact category (Euro/kg)'] #In mln Euro
    data = data[data['Goederengroep_nr'] != 67]
    return data


def visualize_impacts_and_DMI(data, jaar=var.YEAR):
    data = data[data['Jaar'] == jaar]
    # normalise data
    label_names = ['CO2eq uitstoot', 'Milieukostenindicator', 'Direct Material Input']
    col_names = ['CO2 emissions total (kt)', 'MKI total (mln euro)', 'DMI']
    viz_data = pd.DataFrame()
    for i in range(len(col_names)):
        temp = data.pivot_table(index='Regionaam', columns='TA', values=col_names[i], aggfunc='sum')
        temp['type'] = label_names[i]
        temp.set_index('type', inplace=True)
        viz_data = pd.concat([viz_data, temp])
    viz_data = viz_data.astype(float)
    viz_data = viz_data.div(viz_data.sum(axis=1), axis=0)

    viz_data['Kunststoffen'] = 0
    cols = list(viz_data.columns)
    categories = []
    for i in range(len(cols)):
        if len(cols[i].split(', ')) > 1:
            for j in cols[i].split(', '):
                viz_data[j] += 0.5 * viz_data[cols[i]]
        else:
            categories.append(cols[i])
    viz_data = viz_data[categories]
    viz_data.rename(columns={'Bouw': 'Bouwmaterialen', 'Non-specifiek': 'Overig'}, inplace=True)
    viz_data = viz_data[['Biomassa en voedsel',
                         'Kunststoffen',
                         'Bouwmaterialen',
                         'Consumptiegoederen',
                         'Overig',
                         'Maakindustrie']]

    return viz_data


def visualize_impacts(data, indicator = '', col_name='', jaar=var.YEAR):
    data = data[data['Jaar'] == jaar]
    # normalise data
    viz_data = data.pivot_table(values=col_name, index='Regionaam', columns='TA', aggfunc='sum')
    viz_data = viz_data.astype(float)

    viz_data = viz_data.div(viz_data.sum(axis=1), axis=0)

    viz_data['Kunststoffen'] = 0
    cols = list(viz_data.columns)
    categories = []
    for i in range(len(cols)):
        if len(cols[i].split(', ')) > 1:
            for j in cols[i].split(', '):
                viz_data[j] += 0.5 * viz_data[cols[i]]
        else:
            categories.append(cols[i])
    viz_data = viz_data[categories]
    viz_data.rename(columns={'Bouw': 'Bouwmaterialen', 'Non-specifiek': 'Overig'}, inplace=True)
    viz_data = viz_data[['Biomassa en voedsel',
                         'Kunststoffen',
                         'Bouwmaterialen',
                         'Consumptiegoederen',
                         'Overig',
                         'Maakindustrie']]

    return viz_data


def run():
    emissions_file = f'{FILEPATH}/geoFluxus/MKI_CO2_factors.xlsx'
    groups_file = f'{FILEPATH}/geoFluxus/CBS_names.xlsx'
    inds = ['MKI', 'CO2']
    themes = ['environmental_costs', 'co2_impact']
    col_names = ['MKI total (mln euro)', 'CO2 emissions total (kt)']

    # environmental impact of current area
    all_data_file = f'{var.OUTPUT_DIR}/all_data.xlsx'
    dat = calculate_impacts(all_data_file, emissions_file, groups_file)
    dat.to_excel(f'{var.OUTPUT_DIR}/all_impact_data.xlsx')

    # environmental impact of all provinces
    all_prov_data_file = f'{var.OUTPUT_DIR}/all_province_data.xlsx'
    prov_dat = calculate_impacts(all_prov_data_file, emissions_file, groups_file)

    DATA['co2eq_emissions'] = visualize_impacts_and_DMI(dat)
    merged_data = pd.concat([dat, prov_dat])
    for i in [0,1]:
        DATA[themes[i]] = \
            visualize_impacts(merged_data, indicator=inds[i],
                              col_name=col_names[i], jaar=var.YEAR)

    results = {}
    for theme, data in DATA.items():
        values = {}
        agendas = list(data.columns)
        for indicator, row in data.iterrows():
            values[indicator] = [row[agenda] * 100 for agenda in agendas]
        results[theme] = {
            "level": "COROP",
            "name": var.COROPS[0],
            "period": var.YEAR,
            "unit": "%",
            "agendas": agendas,
            "values": values
        }

    return results