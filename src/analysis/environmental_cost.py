import pandas as pd
import variables as var
import numpy as np


FILEPATH = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/monitor_data/data"
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


def get_indicator_per_group(df, on='usage', value_col='CO2 emissions total (kt)'):
    groupby = {
        'usage': 'Gebruiksgroep_naam',
        'agendas': 'TA'
    }

    # Filter year
    df = df[df['Jaar'] == var.YEAR].copy()
    df = df[df['Gebruiksgroep_naam'] != 'Verandering voorraden']

    if on == 'agendas':
        df['TA'] = df['TA'].fillna('')
        df['TA_list'] = df['TA'].str.split(r',\s*')
        df['n_labels'] = df['TA_list'].apply(lambda x: len(x) if x != [''] else 1)
        df['value_split'] = df[value_col] / df['n_labels']
        df = df.explode('TA_list')
        df = df[df['TA_list'] != '']

        group_col = 'TA_list'
        agg_col = 'value_split'
    else:
        group_col = groupby[on]
        agg_col = value_col

    # Group
    group_data = (
        df.groupby(group_col, as_index=False)
          .agg(total=(agg_col, 'sum'))
    )

    # Rename agenda values (after grouping)
    if on == 'agendas':
        group_data[group_col] = group_data[group_col].replace({
            'Bouw': 'Bouwmaterialen',
            'Non-specifiek': 'Overig'
        })

    # ✅ Rename group column using dict key
    group_data = group_data.rename(columns={group_col: on})

    # Total + percentage
    total_sum = group_data['total'].sum()
    group_data['percentage'] = group_data['total'] / total_sum * 100

    return group_data


def run():
    emissions_file = f'{FILEPATH}/geoFluxus/MKI_CO2_factors.xlsx'
    groups_file = f'{FILEPATH}/geoFluxus/CBS_names.xlsx'

    # environmental impact of current area
    all_data_file = f'{var.OUTPUT_DIR}/all_data.xlsx'
    dat = calculate_impacts(all_data_file, emissions_file, groups_file)
    dat.to_excel(f'{var.OUTPUT_DIR}/all_impact_data.xlsx')

    # environmental impact of all provinces
    # all_prov_data_file = f'{var.OUTPUT_DIR}/all_province_data.xlsx'
    # prov_dat = calculate_impacts(all_prov_data_file, emissions_file, groups_file)

    groups = {
        'usage': [
            'Consumptie huishoudens',
            'Dienstverlening bedrijven',
            'Investeringen vaste activa',
            'Overheid',
            'Productie goederen',
        ],
        'agendas': [
            'Biomassa en voedsel',
            'Kunststoffen',
            'Bouwmaterialen',
            'Consumptiegoederen',
            'Overig',
            'Maakindustrie'
        ]
    }

    area = var.COROPS[0]
    sections = {
        'co2eq_emissions': {
            'CO2eq uitstoot': 'CO2 emissions total (kt)',
            'Direct Material Input': 'DMI',
            'Milieukostenindicator': 'MKI total (mln euro)'
        },
        'co2_impact': {
            area: 'CO2 emissions total (kt)'
        },
        'environmental_costs': {
            area: 'MKI total (mln euro)'
        }
    }

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

    for section, indicators in sections.items():
        section_results = []

        for group in ['usage', 'agendas']:
            group_names = groups[group]
            group_values = {}

            for name, indicator in indicators.items():
                data = get_indicator_per_group(dat, on=group, value_col=indicator)

                values = []
                for group_name in group_names:
                    row = data[(data[group] == group_name)]
                    value = row.iloc[0]['percentage'] if not row.empty else 0
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

    return results
