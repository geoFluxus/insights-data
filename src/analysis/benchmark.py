import pandas as pd
import variables as var
from src.analysis import utils


# VARIABLES
VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'YEAR': var.YEAR,
    'OUTPUT_DIR': var.OUTPUT_DIR
}
UNIT = var.UNITS['WASTE']['BENCHMARK']


def filter_by_area(df):
    ROLES = var.ROLES

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # add areas to roles
    source = ROLES['Ontvangst']['source']  # source role
    df = utils.add_areas(df, role=source, areas=polygon, admin_level=VARS['LEVEL'])

    # ONLY PRODUCTION
    return df[df[f"{source}_{VARS['LEVEL']}"] == VARS['AREA']]


def get_potential(df, rladder=None):
    # merge
    df = df.rename(columns={
        'EuralCode': 'eural_code',
        'VerwerkingsmethodeCode': 'processing_code'
    })
    df = pd.merge(df, rladder,
                  how='left', on=['processing_code'])

    return df


def import_dataset(path, area_filter=False):
    df = pd.read_csv(path, low_memory=False)
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)
    if filter_by_area:
        df = filter_by_area(df)

    # aggregate per eural code & process
    groupby = [
        'EuralCode',
        'VerwerkingsmethodeCode'
    ]
    agg = {
        'amount_kg': ('Gewicht_KG', 'sum')
    }
    cols = groupby + [col for col, func in agg.values()]
    eural_processes = df[cols].groupby(by=groupby, as_index=False).agg(**agg)
    eural_processes = eural_processes.rename(columns={
        'EuralCode': 'eural_code',
        'VerwerkingsmethodeCode': 'processing_code'
    })

    return eural_processes


def exclude_rladder_restrictions(df):
    # exclude based on r-ladder restrictions
    # from process to process
    print("Exclude based on r-ladder restrictions...")
    path = f"{var.INPUT_DIR}/DATA/descriptions"
    ref = pd.read_excel(f"{path}/rladder_restrictions.xlsx", sheet_name='Restrictions')

    # apply exceptions
    ref = ref[[
        'code',
        'exception'
    ]].to_records(index=False).tolist()
    df = df[~df[[
        'processing_code_curr',
        'processing_code_alt'
    ]].apply(tuple, axis=1).isin(ref)]

    return df


def exclude_eural_process(df):
    # exclude ewc-process pairs
    print("Exclude eural code & alternative process pairs...")
    path = f"{var.INPUT_DIR}/DATA/descriptions"
    ref = pd.read_excel(f"{path}/alternatives_exclude_processes.xlsx")
    ref = ref[['EuralCode', 'VerwerkingsmethodeCode']]
    ref['EuralCode'] = ref['EuralCode'].astype(str).str.zfill(6)

    # apply exceptions
    ref = ref[[
        'EuralCode',
        'VerwerkingsmethodeCode'
    ]].to_records(index=False).tolist()
    df = df[~df[[
        'eural_code',
        'processing_code_alt'
    ]].apply(tuple, axis=1).isin(ref)]

    return df


def run():
    print("\nWorking on potential sankey...")

    # import rladder
    print("Import rladder...")
    path = f"{var.INPUT_DIR}/DATA/descriptions/rhierarchy.xlsx"
    rladder = pd.read_excel(path)
    rladder = rladder[['processing_code', 'benchmark_group']]
    rladder_names = {
        r['benchmark_group'][0]: r['benchmark_group'][1:].strip()
        for idx, r in rladder.iterrows()
    }
    rladder['benchmark_group'] = rladder['benchmark_group'].str[0]

    # import province dataset
    print(f"\nImport province dataset for {VARS['YEAR']}...")
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
    filename = f"{path}/ontvangst_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
    province_data = get_potential(
        import_dataset(filename, area_filter=True),
        rladder=rladder
    )

    # import national dataset
    print(f"\nImport national dataset for {VARS['YEAR']}...")
    path = f"{VARS['INPUT_DIR']}/DATA/LMA/ontvangst/processed"
    filename = f"{path}/ontvangst_{VARS['YEAR']}_full.csv"
    national_data = get_potential(import_dataset(filename), rladder=rladder)

    # compute potential
    potential = pd.merge(province_data, national_data,
                         how='left',
                         on=['eural_code'],
                         suffixes=['_curr', '_alt'])

    # apply exceptions
    potential = potential[potential['benchmark_group_curr'] != 'I']
    potential = exclude_rladder_restrictions(potential)
    potential = exclude_eural_process(potential)
    potential = potential[
        potential['benchmark_group_curr'] > potential['benchmark_group_alt']
    ]

    # get max rank for each eural code
    groupby = [
        'eural_code',
        'processing_code_curr',
        'benchmark_group_curr',
    ]
    agg = {
        'benchmark_group_alt': ('benchmark_group_alt', 'min'),
    }
    cols = groupby + [col for col, func in agg.values()]
    min_alt = potential[cols].groupby(by=groupby, as_index=False).agg(**agg)

    # match potential with province data
    province_data = pd.merge(province_data, min_alt, how='left',
                             left_on=['eural_code', 'processing_code'],
                             right_on=['eural_code', 'processing_code_curr'])
    # if not matched, keep original benchmark as alternative
    province_data.loc[
        province_data['benchmark_group_alt'].isnull(),
        'benchmark_group_alt'
    ] = province_data['benchmark_group']

    # sankey links
    groupby = [
        'benchmark_group',
        'benchmark_group_alt'
    ]
    agg = {
        'amount_kg': ('amount_kg', 'sum'),
    }
    cols = groupby + [col for col, func in agg.values()]
    links = province_data[cols].groupby(by=groupby, as_index=False, dropna=False).agg(**agg)

    # sankey nodes
    total = links['amount_kg'].sum()
    sources = province_data.groupby(['benchmark_group'], as_index=False).sum('amount_kg')
    sources['pct'] = sources['amount_kg'] / total * 100
    targets = province_data.groupby(['benchmark_group_alt'], as_index=False).sum('amount_kg')
    targets['pct'] = targets['amount_kg'] / total * 100
    targets = targets.rename(columns={'benchmark_group_alt': 'benchmark_group'})

    # export data
    data = {
        'nodes': [],
        'links': []
    }
    for typ, df in zip(['source', 'target'], [sources, targets]):
        data['nodes'].extend([{
            'id': f"{typ}_{n['benchmark_group']}",
            'rank': n['benchmark_group'],
            'name': rladder_names[n['benchmark_group']],
            'value': utils.kg_to_unit(
                n['amount_kg'], unit=UNIT
            ),
            'unit': UNIT,
            'pct': n['pct']
        } for idx, n in df.iterrows()])
    data['links'].extend([{
        'source': f"source_{l['benchmark_group']}",
        'target': f"target_{l['benchmark_group_alt']}",
        'source_rank': l['benchmark_group'],
        'target_rank': l['benchmark_group_alt'],
        'value': utils.kg_to_unit(
            l['amount_kg'], unit=UNIT
        ),
        'unit': UNIT
    } for idx, l in links.iterrows()])

    return data