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
UNIT = var.UNITS['WASTE']['EURAL_TREE']


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


def to_treemap(df):
    hierarchy = {}
    extra = {}
    levels = [
        'chapter',
        'eural'
    ]

    for idx, e in df.iterrows():
        tree = [e[f'{level}_code'] for level in levels]
        tree = utils.build_nested(tree)
        hierarchy = utils.merge_nested(tree, hierarchy)

        for level in levels:
            extra[e[f'{level}_code']] = {
                "name": e[f'{level}_name']
            }
            if level == 'eural':
                extra[e[f'{level}_code']] = {
                    **extra[e[f'{level}_code']],
                    'hazardous': e['hazardous'],
                    'value': utils.kg_to_unit(
                        e['amount_kg'], unit=UNIT
                    ),
                    'unit': UNIT
                }

    tree = utils.update_tree({},
                             hierarchy,
                             extra).get('children', [])
    return tree


def run():
    print("\nWorking on eural treemap...")

    # import eural descriptions
    print("Load eural descriptions...")
    path = f"{VARS['INPUT_DIR']}/DATA/geofluxusApp/templates"
    ewc2 = pd.read_excel(f"{path}/waste02.xlsx")
    ewc2['ewc_code'] = ewc2['ewc_code'].astype(str).str.zfill(2)
    ewc2 = ewc2[['ewc_code', 'ewc_name']].rename(
        columns={'ewc_code': 'chapter_code', 'ewc_name': 'chapter_name'}
    )

    ewc6 = pd.read_excel(f"{path}/waste06.xlsx")
    ewc6['ewc_code'] = ewc6['ewc_code'].astype(str).str.zfill(6)
    ewc6['ewc_name'] = ewc6['ewc_name'].str.capitalize()
    ewc6 = ewc6[['ewc_code', 'ewc_name', 'hazardous']].rename(
        columns={'ewc_code': 'eural_code', 'ewc_name': 'eural_name'}
    )

    # import province dataset
    print(f"\nImport province data for {VARS['YEAR']}...")
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
    filename = f"{path}/ontvangst_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
    df = pd.read_csv(filename, low_memory=False)
    df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)
    print(f"\nFilter on production only within area...")
    df = filter_by_area(df)

    # aggregate per eural code
    groupby = [
        'EuralCode'
    ]
    agg = {
        'amount_kg': ('Gewicht_KG', 'sum')
    }
    cols = groupby + [col for col, func in agg.values()]
    eurals = df[cols].groupby(by=groupby, as_index=False).agg(**agg)

    # get top 20 streams by amount
    eurals = eurals.sort_values(by=['amount_kg'], ascending=False)[:20]

    # add descriptions
    eurals = eurals.rename(columns={'EuralCode': 'eural_code'})
    eurals['chapter_code'] = eurals['eural_code'].str[:2]
    eurals = pd.merge(eurals, ewc2, how='left', on='chapter_code')
    eurals = pd.merge(eurals, ewc6, how='left', on='eural_code')

    # add to data
    return to_treemap(eurals)