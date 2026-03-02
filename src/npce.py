import json
import pandas as pd
import variables as var


BEGIN = 2016
HUIDIG = var.YEAR

DATA = {
    'indicators': {}
}


def perc(curr, ref):
    return curr / ref * 100 if ref != 0 else 0


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
    for year in range(BEGIN, HUIDIG + 1):
        renew_sum = renewable_sum(df[df['Jaar'] == year])
        total_sum = df[df['Jaar'] == year]['DMI'].sum()
        graph['data'].append({
            "year": year,
            "renew": perc(renew_sum, total_sum),
            "other": perc(total_sum - renew_sum, total_sum),
            "unit": "%"
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
            'total': begin_total_sum,
            'raw': begin_sum,
            'reduction': begin_total_sum - begin_sum
        },
        'curr': {
            'total': begin_total_sum,
            'raw': huidig_sum,
            'reduction': begin_total_sum - huidig_sum
        },
        'goals': {
            'begin': {
                'total': begin_total_sum,
                'raw': begin_total_sum * (100 - g2030) / 100,
                'reduction': begin_total_sum * g2030 / 100
            },
            'curr': {
                'total': begin_total_sum,
                'raw': begin_total_sum * (100 - g2035) / 100,
                'reduction': begin_total_sum * g2035 / 100
            },
            'unit': 'kt'
        },
        'unit': 'kt'
    }

    graph = {
        'data': [],
        'unit': 'kt',
        'targets': [
            {'value': begin_total_sum * (100 - g2030) / 100},
            {'value': begin_total_sum * (100 - g2035) / 100},
        ]
    }
    for year in range(BEGIN, HUIDIG + 1):
        total_sum = df[df['Jaar'] == year]['DMI'].sum()
        graph['data'].append({
            "year": year,
            "raw": total_sum,
            "unit": "%"
        })

    return indicator, graph


def compute_goederen():
    # add NON_FE (non-fossil) data
    path = fr"{var.OUTPUT_DIR}/all_data.xlsx"
    df = pd.read_excel(path, sheet_name='NON_FE')

    # merge with renewable
    path = fr"{var.INPUT_DIR}\Database_LockedFiles\DATA\ontology\cbs_renewable.xlsx"
    renewable = pd.read_excel(path)
    goederen = pd.merge(df, renewable, on='cbs')

    # compute goals
    for goal, func in [
        ('vervangen', vervangen),
        ('besparen', besparen)
    ]:
        indicator, graph = func(goederen)
        DATA['indicators'][goal] = indicator
        DATA[goal] = graph


if __name__ == '__main__':
    # compute goederen
    compute_goederen()

    with open(f"{var.OUTPUT_DIR}/npce.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)
