import json
import pandas as pd
import variables as var
from src.analysis import utils


BEGIN = 2016
HUIDIG = var.YEAR
YEARS = range(BEGIN, HUIDIG + 1)

DATA = {
    'indicators': {}
}


def format_num(num):
    return round(num)


def perc(curr, ref):
    return round(curr / ref * 100) if ref != 0 else 0


def renewable_sum(df):
    return (
        df.loc[df["renewable"].isin(["hernieuwbaar", "secundair"]), "DMI"].sum()
        + 0.5 * df.loc[df["renewable"] == "gemengd", "DMI"].sum()
    )


def vervangen(df):
    begin_sum = renewable_sum(df[df['Jaar'] == BEGIN])
    begin_total_sum = df[df['Jaar'] == BEGIN]['DMI'].sum()
    huidig_sum = renewable_sum(df[df['Jaar'] == HUIDIG])
    huidig_total_sum = df[df['Jaar'] == HUIDIG]['DMI'].sum()
    indicator = {
        'begin': {
            'renew': perc(begin_sum, begin_total_sum),
            'other': perc(begin_total_sum - begin_sum, begin_total_sum)
        },
        'curr': {
            'renew': perc(huidig_sum, huidig_total_sum),
            'other': perc(huidig_total_sum - huidig_sum, huidig_total_sum)
        },
        'unit': '%'
    }

    graph = {
        'data': []
    }
    for year in YEARS:
        cats, year_df = utils.split_categories(
            df[df['Jaar'] == year],
            column='productgroepen',
            extra=['renewable'],
            amount='DMI'
        )

        for cat in cats:
            cat_df = year_df[year_df['productgroepen'] == cat]
            renew_sum = renewable_sum(cat_df)
            total_sum = cat_df['DMI'].sum()
            graph['data'].append({
                "year": year,
                "group": cat,
                "renew": renew_sum,
                "other": total_sum - renew_sum,
                "unit": "kt"
            })

    return indicator, graph


def besparen(df):
    g2030 = 5
    g2035 = 16
    begin_sum = df[df['Jaar'] == BEGIN]['DMI'].sum()
    begin_total_sum = df[df['Jaar'] == BEGIN]['DMI'].sum()
    huidig_sum = df[df['Jaar'] == HUIDIG]['DMI'].sum()

    indicator = {
        'begin': {
            'total': format_num(begin_total_sum),
            'raw': format_num(begin_sum),
            'reduction': format_num(begin_total_sum - begin_sum)
        },
        'curr': {
            'total': format_num(begin_total_sum),
            'raw': format_num(huidig_sum),
            'reduction': format_num(begin_total_sum - huidig_sum)
        },
        'goals': {
            'begin': {
                'total': format_num(begin_total_sum),
                'raw': format_num(begin_total_sum * (100 - g2030) / 100),
                'reduction': format_num(begin_total_sum * g2030 / 100)
            },
            'curr': {
                'total': format_num(begin_total_sum),
                'raw': format_num(begin_total_sum * (100 - g2035) / 100),
                'reduction': format_num(begin_total_sum * g2035 / 100)
            },
            'unit': 'kt'
        },
        'unit': 'kt'
    }

    graph = {
        'data': [],
        'unit': 'kt',
        'targets': [
            {'value': format_num(begin_total_sum * (100 - g2030) / 100)},
            {'value': format_num(begin_total_sum * (100 - g2035) / 100)},
        ]
    }
    for year in YEARS:
        cats, year_df = utils.split_categories(
            df[df['Jaar'] == year],
            column='productgroepen',
            amount='DMI'
        )

        for cat in cats:
            cat_df = year_df[year_df['productgroepen'] == cat]
            total_sum = cat_df['DMI'].sum()
            graph['data'].append({
                "year": year,
                "group": cat,
                "raw": total_sum,
                "unit": "kt"
            })

    return indicator, graph


def compute_goederen():
    # add NON_FE (non-fossil) data
    path = fr"{var.OUTPUT_DIR}/all_data.xlsx"
    df = pd.read_excel(path, sheet_name='NON_FE')

    # merge with renewable
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_hernieuwbaar.xlsx"
    renewable = pd.read_excel(path)
    goederen = pd.merge(df, renewable, on='cbs')

    # merge with product groups
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_productgroepen.xlsx"
    productgroups = pd.read_excel(path, sheet_name='goederen')
    goederen = pd.merge(goederen, productgroups, on='cbs')

    # compute goals
    for goal, func in [
        ('vervangen', vervangen),
        ('besparen', besparen)
    ]:
        indicator, graph = func(goederen)
        DATA['indicators'][goal] = indicator
        DATA[goal] = graph


def filter_by_area(df):
    ROLES = var.ROLES

    # import areas
    # import province polygon
    polygon = utils.import_areas(level=var.LEVEL)
    polygon = polygon[polygon['name'] == var.AREA]
    assert len(polygon) == 1

    # add areas to roles
    source = ROLES['Ontvangst']['source']  # source role
    df = utils.add_areas(
        df,
        role=source,
        areas=polygon,
        admin_level=var.LEVEL
    )

    # ONLY PRODUCTION
    return df[df[f"{source}_{var.LEVEL}"] == var.AREA]


def get_process_sum(df, process=None):
    return df[
        df['Berekening NPCE doelstellingen'] == process
    ]['Gewicht_kt'].sum()


def behouden_hoeveelheid(df):
    g2030 = 15
    g2035 = 15
    begin_sum = df[df['MeldPeriodeJAAR'] == BEGIN]['Gewicht_kt'].sum()
    begin_total_sum = df[df['MeldPeriodeJAAR'] == BEGIN]['Gewicht_kt'].sum()
    huidig_sum = df[df['MeldPeriodeJAAR'] == HUIDIG]['Gewicht_kt'].sum()

    indicator = {
        'begin': {
            'total': format_num(begin_total_sum),
            'raw': format_num(begin_sum),
            'reduction': format_num(begin_total_sum - begin_sum)
        },
        'curr': {
            'total': format_num(begin_total_sum),
            'raw': format_num(huidig_sum),
            'reduction': format_num(begin_total_sum - huidig_sum)
        },
        'goals': {
            'begin': {
                'total': format_num(begin_total_sum),
                'raw': format_num(begin_total_sum * (100 - g2030) / 100),
                'reduction': format_num(begin_total_sum * g2030 / 100)
            },
            'curr': {
                'total': format_num(begin_total_sum),
                'raw': format_num(begin_total_sum * (100 - g2035) / 100),
                'reduction': format_num(begin_total_sum * g2035 / 100)
            },
            'unit': 'kt'
        },
        'unit': 'kt'
    }

    graph = {
        'data': [],
        'unit': 'kt',
    }

    for year in YEARS:
        cats, year_df = utils.split_categories(
            df[df['MeldPeriodeJAAR'] == year],
            column='productgroepen',
            extra=['Berekening NPCE doelstellingen'],
            amount='Gewicht_kt'
        )

        for cat in cats:
            cat_df = year_df[year_df['productgroepen'] == cat]
            graph['data'].append({
                "year": year,
                "group": cat,
                'high': get_process_sum(cat_df, process='Hoogwaardige recycling'),
                'other': get_process_sum(cat_df, process='Recycling'),
                'low': get_process_sum(cat_df, process='Verbranding / storting'),
                "unit": "kt"
            })

    return indicator, graph


def behouden_verwerking(df):
    begin = df[df['MeldPeriodeJAAR'] == BEGIN]
    begin_total_sum = begin['Gewicht_kt'].sum()
    huidig = df[df['MeldPeriodeJAAR'] == HUIDIG]
    huidig_total_sum = huidig['Gewicht_kt'].sum()

    indicator = {
        'begin': {
            'high': perc(
                get_process_sum(begin, process='Hoogwaardige recycling'),
                begin_total_sum
            ),
            'other': perc(
                get_process_sum(begin, process='Recycling'),
                begin_total_sum
            ),
            'low': perc(
                get_process_sum(begin, process='Verbranding / storting'),
                begin_total_sum
            )
        },
        'curr': {
            'high': perc(
                get_process_sum(huidig, process='Hoogwaardige recycling'),
                huidig_total_sum
            ),
            'other': perc(
                get_process_sum(huidig, process='Recycling'),
                huidig_total_sum
            ),
            'low': perc(
                get_process_sum(huidig, process='Verbranding / storting'),
                huidig_total_sum
            )
        },
        'unit': '%'
    }

    graph = None

    return indicator, graph


def compute_afval():
    # import lma datasets (production only)
    concats = []
    path = fr"{var.INPUT_DIR}\Monitors\{var.LEVEL}{var.AREA}\LMA\processed"
    for year in YEARS:
        filename = fr"{path}\ontvangst_{var.AREA.lower()}_{year}_full.csv"
        df = pd.read_csv(filename, low_memory=False)
        prod_only = filter_by_area(df)
        prod_only['Gewicht_kt'] = prod_only['Gewicht_KG'] / 10 ** 6
        concats.append(prod_only)
    afval = pd.concat(concats)
    afval['EuralCode'] = afval['EuralCode'].astype(str).str.zfill(6)

    # import process value
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_hoogwaardig.xlsx"
    process = pd.read_excel(path)
    afval = pd.merge(afval, process,
                     left_on='VerwerkingsmethodeCode',
                     right_on='LMA verwerkingscode')

    # import process value
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\npce_productgroepen.xlsx"
    productgroups = pd.read_excel(path, sheet_name='afval')
    productgroups['ewc'] = productgroups['ewc'].astype(str).str.zfill(6)
    afval = pd.merge(afval, productgroups,
                     left_on='EuralCode',
                     right_on='ewc')

    # compute goals
    for goal, func in [
        ('behouden_hoeveelheid', behouden_hoeveelheid),
        ('behouden_verwerking', behouden_verwerking)
    ]:
        indicator, graph = func(afval)
        DATA['indicators'][goal] = indicator
        DATA[goal] = graph


if __name__ == '__main__':
    compute_goederen()
    compute_afval()

    with open(f"{var.OUTPUT_DIR}/npce.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)
