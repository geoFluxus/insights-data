import pandas as pd
import variables as var


FILEPATH = f"{var.INPUT_DIR}/Database_LockedFiles/DATA/monitor_data/data"
crm_names = ['Antimoon', 'Beryllium', 'Chroom', 'Kobalt', 'Cokeskolen', 'Fluoriet', 'Fosfor', 'Indium', 'Lithium',
             'Molybdeen', 'Grafiet', 'Niobium', 'Silicium', 'Zilver', 'Tin', 'Titanium', 'Wolfraam', 'Vanadium', 'Zink',
             'Aluminium', 'Barieten', 'Bentoniet', 'Boor', 'Koper', 'Diatomiet', 'Veldspaat', 'Gallium', 'Germanium',
             'Goud', 'Gips', 'Ijzer', 'Kaolien', 'Kalksteen', 'Magnesium', 'Mangaan', 'Nikkel', 'Perliet', 'Rhenium',
             'Selenium', 'Silicazand', 'Strontium', 'Talk', 'Tantalum', 'Tellurium', 'Uranium', 'Zirconium', 'Iridium',
             'Osmium', 'Palladium', 'Platina', 'Rhodium', 'Ruthenium', 'Cerium',
             'Europium', 'Gadolinium', 'Lanthanum', 'Neodymium', 'Praseodymium',
             'Samarium', 'Scandium', 'Dysprosium', 'Terbium', 'Ytterbium', 'Yttrium']
materials = None


def correct_car_crm_fractions(data):
    cats = [f'870{i}' for i in range(1,6)]
    print(data['Samarium'].sum())
    data.loc[data['gn_code'].astype(str).str[:4].isin(cats),crm_names] = data[data['gn_code'].astype(str) == '87032190'][crm_names].values[0]
    print(data['Samarium'].sum())
    return data


def calculate_crm_shares_per_province():
    cn_code_col = 'CN2020_CODE'

    crm_contents = pd.read_csv(f'{FILEPATH}/TNO/CN_CRM_typical_shares.csv', delimiter=';', decimal=',')
    crm_contents[crm_contents.isna()] = 0
    crm_contents = correct_car_crm_fractions(crm_contents)
    good_weights = pd.read_excel(f'{FILEPATH}/TNO/CN_goederen_totalen_2020.xlsx', sheet_name='Goederen_totalen_2020')
    good_weights = good_weights[['CN_8D', 'Final_count_kg']]
    good_weights['CN_8D'] = good_weights['CN_8D'].astype(str)

    cn_to_nst_code = pd.read_excel(f'{FILEPATH}/geoFluxus/NST2007_CN2020_Table.xlsx')
    cn_to_nst_code = cn_to_nst_code[[cn_code_col, 'NST2007_CODE']]
    cn_to_nst_code[cn_code_col] = cn_to_nst_code[cn_code_col].str.replace(' ', '')
    # print(cn_to_nst_code[cn_to_nst_code[cn_code_col].astype(str).str.len() != 8])
    # print(cn_to_nst_code)
    #cn_to_nst_code[cn_code_col] = cn_to_nst_code['CN2024_CODE'].astype(str).zfill(8)
    dmi = pd.read_excel(f'{var.OUTPUT_DIR}/all_data.xlsx')[['Goederengroep', 'Regionaam', 'Jaar', 'DMI']]
    dmi = dmi[dmi['Jaar'] == var.YEAR]


    good_weights = pd.merge(good_weights, cn_to_nst_code, how='left', left_on='CN_8D', right_on=cn_code_col).drop(columns=cn_code_col)
    cn_code_col = 'CN_8D'
    # print(good_weights['Final_count_kg'].sum())
    # print(good_weights['Final_count_kg'][good_weights['CN_8D'].isna() | good_weights[cn_code_col].isna()].sum())
    good_weights['Final_count_kg'][good_weights['Final_count_kg'].isna()] = 0

    cbs_names_to_nst = pd.read_excel(f'{FILEPATH}/geoFluxus/CBS_names.xlsx', sheet_name='CBS_code_merger')
    cbs_names_to_nst = cbs_names_to_nst[['Goederengroep_naam', 'NST_code']]
    print(cbs_names_to_nst)
    nst_total_weights = good_weights.groupby('NST2007_CODE')['Final_count_kg'].sum()
    nst_total_weights.name = 'total_weight_per_nst_code'
    good_weights = pd.merge(good_weights, nst_total_weights, how='left', left_on='NST2007_CODE', right_index=True)
    #print(good_weights.columns)
    # print(good_weights.columns)
    # print(good_weights['Final_count_kg'][good_weights['NST2007_CODE']=='01.8'].sum())
    # print(good_weights['total_weight_per_nst_code'].isna().sum())
    good_weights['good_distribution_per_nst'] = good_weights['Final_count_kg'] / good_weights['total_weight_per_nst_code']
    good_weights['good_distribution_per_nst'][good_weights['good_distribution_per_nst'].isna()] = 0

    good_weights = good_weights[~good_weights['NST2007_CODE'].isna()]
    # for i in good_weights['NST2007_CODE'].unique():
    #     if good_weights['good_distribution_per_nst'][good_weights['NST2007_CODE'] == i].sum() != 1:
    #         print(i, good_weights['good_distribution_per_nst'][good_weights['NST2007_CODE'] == i].sum())

    crm_contents = crm_contents[crm_contents['gn_code'].astype(str) != '1022905'].astype(str)

    crm_in_goods = pd.merge(good_weights, crm_contents, how='left', left_on=cn_code_col, right_on='gn_code')
    crm_in_goods[crm_in_goods.isna()] = 0
    #print(crm_in_goods.columns)


    for i in crm_names:
        crm_in_goods[i] = crm_in_goods[i].astype(float) * crm_in_goods['good_distribution_per_nst']
    crm_in_goods.to_excel(f'{FILEPATH}/goods_crm_fractions.xlsx')
    crm_per_nst_code = crm_in_goods.groupby('NST2007_CODE')[crm_names].sum()
    #print(crm_per_nst_code)
    crm_per_nst_code.to_excel(f'{FILEPATH}/crm_fractions.xlsx')
    dmi = pd.merge(dmi, cbs_names_to_nst, left_on='Goederengroep', right_on='Goederengroep_naam')

    dmi['NST_code'] = dmi['NST_code'].str.split(', ')

    # Store the original values in columns B and C before exploding
    dmi['num_categories'] = dmi['NST_code'].apply(len)

    # Explode the DataFrame to create a new row for each category in the list
    dmi = dmi.explode('NST_code')

    # Divide the values in columns B and C by the number of categories
    dmi['DMI'] = dmi['DMI'] / dmi['num_categories']
    # Drop the helper column
    dmi = dmi.drop(columns='num_categories')

    crm_per_province = pd.merge(dmi, crm_per_nst_code, how='left', left_on='NST_code', right_index=True)
    crm_per_province[crm_per_province.isna()] = 0
    for i in crm_names:
        crm_per_province[i] = crm_per_province[i] * crm_per_province['DMI'] # now in TONNES (g/kg * mln kg)
    print(crm_per_province.columns)
    agg_dict = {
        'Goederengroep_naam': 'first',
        'NST_code': 'first',
        'Goederengroep': 'first',
        'Regionaam': 'first',
        'Jaar': 'first'
    }
    for col in crm_per_province.columns:
        if col not in agg_dict:
            agg_dict[col] = 'sum'
    crm_per_province = crm_per_province.groupby(['Goederengroep', 'Regionaam', 'Jaar'], as_index=False).agg(agg_dict)

    return crm_per_province


def plot_heatmap(dat, mat_inds, prov=None, values=None):
    indicators = mat_inds[mat_inds['Materiaal'].isin(materials)]
    indicators = indicators.sort_values(by='product', ascending=False)
    indicators = indicators[~indicators['Materiaal'].isna()]
    if values is not None:
        values = values.sort_values(by='Inkoop_waarde', ascending=True)
    viz_data = dat[dat['Regionaam'] == prov]
    viz_data.index = viz_data['Goederengroep']
    viz_data = viz_data[list(indicators['Materiaal'])]
    viz_data = viz_data / viz_data.sum()
    viz_data['row_sum'] = viz_data.sum(axis=1)
    viz_data = pd.merge(viz_data, values[values['Regionaam'] == prov][['Goederengroep', 'Inkoop_waarde']],
                            on= 'Goederengroep', how='left')
    viz_data = viz_data.sort_values(by='Inkoop_waarde', ascending=False)

    return viz_data


def export_overview(viz_data):
    total_value = viz_data['Inkoop_waarde'].sum()
    viz_data = viz_data[viz_data['row_sum'] != 0]
    viz_data['Invoerwaarde'] = viz_data['Inkoop_waarde'] / total_value

    overview_data = []
    for idx, row in viz_data.iterrows():
        overview_data.append({
            "material": row['Goederengroep'],
            "crm": row['row_sum'] / len(crm_names) * 100,
            "value": row['Invoerwaarde'] * 100
        })

    return {
        "level": var.PREFIXES[var.LEVEL],
        "period": var.YEAR,
        "name": var.AREA,
        "unit": "%",
        "values": overview_data
    }


def export_heatmap(viz_data):
    # export data
    viz_data = viz_data[viz_data['row_sum'] != 0]
    heatmap_materials = [col for col in viz_data.columns if col in materials]
    heatmap_data = {}
    for idx, row in viz_data.iterrows():
        data = {
            'worth': row['Inkoop_waarde'],
            'amounts': []
        }
        for material in heatmap_materials:
            data['amounts'].append(
                row[material] * 100 if row[material] > 0 else None
            )
        heatmap_data[row['Goederengroep']] = data

    return {
        "level": var.PREFIXES[var.LEVEL],
        "period": var.YEAR,
        "name": var.AREA,
        "unit": "%",
        "materials": heatmap_materials,
        "values": heatmap_data
    }


def run():
    # CALCULATE DATA
    data = calculate_crm_shares_per_province()
    data.to_excel(f'{var.OUTPUT_DIR}/material_contents.xlsx')

    indicators = pd.read_excel(f'{FILEPATH}/geoFluxus/EU CRM table.xlsx')
    indicators['product'] = indicators['Economic Importance (EI)'] * indicators['Supply Risk (SR)']

    # PLOT MATERIALS
    plt_indicators = indicators.copy()
    plt_indicators = plt_indicators.sort_values('product')
    plt_indicators = plt_indicators[~plt_indicators['Materiaal'].isna()]
    criticals = plt_indicators[(indicators['Economic Importance (EI)'] >= 2.8) & (indicators['Supply Risk (SR)'] >= 1)]
    global materials
    materials = list(criticals['Materiaal'].dropna())

    euro_waarde = pd.read_excel(f'{var.OUTPUT_DIR}/euro_data_all.xlsx')
    euro_waarde = euro_waarde[euro_waarde['Jaar'] == var.YEAR]
    euro_waarde['Inkoop_waarde'] = euro_waarde['Invoer_nationaal'] + euro_waarde['Invoer_internationaal']
    euros = euro_waarde[['Regionaam', 'Goederengroep', 'Inkoop_waarde']]

    # PLOT MATERIALS
    viz_data = plot_heatmap(data, indicators, prov=var.COROPS[0], values=euros)

    return {
        'material_overview': export_overview(viz_data),
        'raw_materials': export_heatmap(viz_data)
    }
