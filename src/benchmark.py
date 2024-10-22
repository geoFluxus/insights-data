import pandas as pd
import variables as var
import utils


# VARIABLES
VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'YEAR': var.YEAR,
    'OUTPUT_DIR': var.OUTPUT_DIR
}

DATA = {}


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
                    'value': utils.kg_to_unit(
                        e['amount_kg'] / 10**3, unit='t'
                    ),
                    'unit': 't'
                }

    tree = utils.update_tree({},
                             hierarchy,
                             extra).get('children', [])
    return tree


def treemap():
    print("\nWorking on eural treemap...")

    # import eural descriptions
    path = f"{VARS['INPUT_DIR']}/DATA/geofluxusApp/templates"
    ewc2 = pd.read_excel(f"{path}/waste02.xlsx")
    ewc2['ewc_code'] = ewc2['ewc_code'].astype(str).str.zfill(2)
    ewc2 = ewc2[['ewc_code', 'ewc_name']].rename(
        columns={'ewc_code': 'chapter_code', 'ewc_name': 'chapter_name'}
    )

    ewc6 = pd.read_excel(f"{path}/waste06.xlsx")
    ewc6['ewc_code'] = ewc6['ewc_code'].astype(str).str.zfill(6)
    ewc6['ewc_name'] = ewc6['ewc_name'].str.capitalize()
    ewc6 = ewc6[['ewc_code', 'ewc_name']].rename(
        columns={'ewc_code': 'eural_code', 'ewc_name': 'eural_name'}
    )

    for typ in ['Ontvangst']:
        # import dataset
        print(f'\nImport {typ}...')
        path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
        filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
        df = pd.read_csv(filename, low_memory=False)

        # fix eural codes
        df['EuralCode'] = df['EuralCode'].astype(str).str.zfill(6)

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



