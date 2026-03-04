import pandas as pd
import variables as var
import numpy as np


FILEPATH = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/monitor_data/data"
DATA = {}


def calculate_impacts(data_file='', impact_file='', group_relation_file=''):
    data = pd.read_excel(data_file)
    impacts = pd.read_excel(impact_file).drop(columns='Unnamed: 0')
    ta = pd.read_excel(group_relation_file, sheet_name='goederen')

    data = pd.merge(data, impacts, how='left', left_on='cbs', right_on='Goederengroep_code')
    data = pd.merge(data, ta[['cbs', 'productgroepen']], how='left', on='cbs')
    data['CO2 emissions (kg CO2e/kg)'] = data['CO2 emissions (kg CO2e/kg)'].astype(float)
    data['Impact category (Euro/kg)'] = data['Impact category (Euro/kg)'].astype(float)

    data['CO2 emissions total (kt)'] = data['DMI'] * data['CO2 emissions (kg CO2e/kg)']  #In kt
    data['MKI total (mln euro)'] = data['DMI'] * data['Impact category (Euro/kg)'] #In mln Euro

    return data


def get_indicator_per_group(df, on='usage', value_col='CO2 emissions total (kt)', perc=False):
    groupby = {
        'usage': 'Gebruiksgroep_naam',
        'product': 'productgroepen'
    }

    # Filter year
    if perc:
        df = df[df['Jaar'] == var.YEAR]
        df = df[df['Gebruiksgroep_naam'] != 'Verandering voorraden']

    if on == 'product':
        df['TA'] = df['productgroepen'].fillna('')
        df['TA_list'] = df['TA'].str.split('&')
        df['n_labels'] = df['TA_list'].apply(lambda x: len(x) if x != [''] else 1)
        df['value_split'] = df[value_col] / df['n_labels']
        df = df.explode('TA_list')
        df = df[df['TA_list'] != '']
        df['TA_list'] = df['TA_list'].map(var.PRODUCTGROEPEN)

        group_col = 'TA_list'
        agg_col = 'value_split'
    else:
        group_col = groupby[on]
        agg_col = value_col

    # Group
    group_data = (
        df.groupby(by=[group_col, 'Jaar'], as_index=False)
          .agg(value=(agg_col, 'sum'))
    )

    # ✅ Rename group column using dict key
    group_data = group_data.rename(columns={group_col: on})

    # Total + percentage
    if perc:
        total_sum = group_data['value'].sum()
        group_data['value'] = group_data['value'] / total_sum * 100

    return group_data


def run():
    emissions_file = f'{FILEPATH}/geoFluxus/MKI_CO2_factors.xlsx'
    groups_file = fr'{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_productgroepen.xlsx'

    # environmental impact of current area
    all_data_file = f'{var.OUTPUT_DIR}/all_data.xlsx'
    dat = calculate_impacts(all_data_file, emissions_file, groups_file)
    dat.to_excel(f'{var.OUTPUT_DIR}/all_impact_data.xlsx')

    # highlights
    curr_year_data = dat[dat['Jaar'] == var.YEAR].copy()
    results = {
        'highlights': {
            'co2_emissions': {
                'value': curr_year_data['CO2 emissions total (kt)'].sum(),
                'unit': 'kt'
            },
            'mki': {
                'value': curr_year_data['MKI total (mln euro)'].sum(),
                'unit': 'mln €'
            }
        }
    }

    groups = {
        'usage': [
            'Consumptie huishoudens',
            'Dienstverlening bedrijven',
            'Investeringen vaste activa',
            'Overheid',
            'Productie goederen',
        ],
        'product': list(dict.fromkeys(var.PRODUCTGROEPEN.values()))
    }

    percentage = {
        'co2eq_emissions': {
            'Direct Material Input': 'DMI',
            'CO2eq uitstoot': 'CO2 emissions total (kt)',
            'Milieukostenindicator': 'MKI total (mln euro)'
        },
    }
    for section, indicators in percentage.items():
        section_results = []

        for group in ['usage', 'product']:
            group_names = groups[group]
            group_values = {}

            for name, indicator in indicators.items():
                data = get_indicator_per_group(dat, on=group, value_col=indicator, perc=True)

                values = []
                for group_name in group_names:
                    row = data[(data[group] == group_name)]
                    value = row.iloc[0]['value'] if not row.empty else 0
                    values.append(value)

                group_values[name] = values

            section_results.append({
                "level": "COROP",
                "name": var.COROPS[0],
                "unit": '%',
                "on": group,
                "groups": group_names,
                "values": group_values
            })

        results[section] = section_results

    totals = {
        'dmi': ('DMI', 'kt'),
        'co2': ('CO2 emissions total (kt)', 'kt'),
        'mki': ('MKI total (mln euro)', 'mln euro')
    }
    for section, indicator in totals.items():
        section_results = []

        for group in ['usage', 'product']:
            group_names = groups[group].copy()
            if group == 'usage':
                group_names.append('Verandering voorraden')
            value, unit = indicator
            data = get_indicator_per_group(dat, on=group, value_col=value, perc=False)

            values = []
            for year in var.DMI_YEARS:
                year_values = []
                for group_name in group_names:
                    row = data[
                        (data[group] == group_name) & \
                        (data['Jaar'] == year)
                    ]
                    value = row.iloc[0]['value'] if not row.empty else 0
                    year_values.append(value)
                values.append(year_values)

            group_values = values

            section_results.append({
                "level": "COROP",
                "name": var.COROPS[0],
                "unit": unit,
                "on": group,
                "groups": group_names,
                "years": var.DMI_YEARS,
                "values": group_values
            })

        results[section] = section_results

    return results
