import pandas as pd
import numpy as np
from src.analysis import utils
import variables as var
import matplotlib.pyplot as plot


VARS = {
    'INPUT_DIR': var.INPUT_DIR,
    'AREA_DIR': var.AREA_DIR,
    'AREA': var.AREA,
    'LEVEL': var.LEVEL,
    'COROPS': var.COROPS,
    'COROP_FILE': var.COROP_FILE,
    'POSTCODES': var.POSTCODES,
    'YEAR': var.YEAR,
    'OUTPUT_DIR': var.OUTPUT_DIR,
    'COMPANY_WASTE_UNIT': var.UNITS['OVERVIEW']['HIGHLIGHTS']
}

GOODS = None
HOUSEHOLD = None
LMA = None
DATA = {}


def to_dec(num):
    return round(num, 1)


def import_household_data(areas=None):
    """
    Import & ready CBS household data for processing
    """

    # add gemeente & provincie
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/CBS"
    df = pd.read_excel(f"{path}/Huishoudelijk_Gemeenten.xlsx", sheet_name='Data')
    columns = list(df.columns)
    df = df.replace('?', np.nan)
    df = pd.merge(df, areas, left_on='Gebieden', right_on='Gemeente', how='left')
    columns.append('Provincie')
    df = df[columns]

    # import population data
    population = pd.read_csv(f"{path}/populationNL.csv", delimiter=';')

    # add population
    def add_population(row):
        gemeente, year = row['Gebieden'], row['Perioden']
        res = population[population['Gemeente'] == gemeente][str(year)]
        if not res.empty:
            res = res.values[0]
            return res
        return np.nan
    df['Inwoners'] = df.apply(lambda x: add_population(x), axis=1)

    return df


def cbs_primary_waste(input):
    df = input.copy()
    df['Gewicht_KG'] = df["Totaal aangeboden huishoudelijk afval [Kilo's per inwoner]"] * df['Inwoners']
    result = df['Gewicht_KG'].sum()

    return result


def total_imported_machine_apparatus():
    machine_codes = [
        50,  # Machines en werktuigen voor de land- en bosbouw
        51,  # Huishoudapparaten (witgoed)
        52,  # Kantoormachines en computers
        53,  # Overige elektrische machines en apparaten
        54,  # Elektronische onderdelen en zend- en transmissietoestellen
        55,  # Televisie- en radiotoestellen, audio- en videoapparatuur (bruingoed)
        56,  # Medische, precisie- en optische instrumenten
        57,  # Overige machines en gereedschapswerktuigen
    ]

    if len(VARS['COROPS']):
        imported_goods = GOODS[
            GOODS['Stroom'].isin([
                'Invoer_internationaal',
                'Invoer_nationaal'
            ])
        ]
        machines = imported_goods[
            imported_goods['Goederengroep_nr'].isin(machine_codes)
        ]
        machines_worth = machines['Waarde'].sum()
        imported_goods_worth = imported_goods['Waarde'].sum()
        perc = machines_worth / imported_goods_worth * 100
        print(f"{to_dec(perc)}% ({to_dec(machines_worth / 10 ** 3)} md) "
              f"van de totale importwaarde waren machines en apparaten")
        DATA['import_worth'] = {
            "amount": to_dec(machines_worth / 10 ** 3),
            "unit": "md",
            "pct": to_dec(perc)
        }


def total_exported_food():
    food_codes = [
        20,  # Vlees, ongelooide huiden en vellen en vleesproducten
        21,  # Vis en visproducten, verwerkt en verduurzaamd
        22,  # Groenten en fruit, verwerkt en verduurzaamd
        23,  # Dierlijke en plantaardige oliën en vetten
        24,  # Zuivelproducten en consumptie - ijs
        25,  # Maalderijproducten, zetmeel en zetmeelproducten; bereide diervoeders
        26,  # Dranken
        27,  # Overige voedingsmiddelen en tabaksproducten
    ]

    exported_goods = GOODS[
        GOODS['Stroom'].isin([
            'Uitvoer_internationaal',
            'Uitvoer_nationaal'
        ])
    ]
    food = exported_goods[
        exported_goods['Goederengroep_nr'].isin(food_codes)
    ]
    food_worth = food['Waarde'].sum()
    exported_goods_worth = exported_goods['Waarde'].sum()
    perc = food_worth / exported_goods_worth * 100
    print(f"{to_dec(perc)}% ({to_dec(food_worth / 10 ** 3)} md) "
          f"van de totale exportwaarde was van voedsel")
    DATA['export_worth'] = {
        "amount": to_dec(food_worth / 10 ** 3),
        "unit": "md",
        "pct": to_dec(perc)
    }


def waste_produced_by_companies():
    lma = LMA.copy()
    lma['EC2'] = lma['EuralCode'].astype(str).str.zfill(6).str[:2]
    lma = lma[
        (lma[f"Herkomst_{VARS['LEVEL']}"] == VARS['AREA']) &
        (lma['EC2'] != '19')
        ]  # all waste produced except chapter 19
    company_amount = lma['Gewicht_KG'].sum()
    total_amount = company_amount + HOUSEHOLD
    perc = company_amount / total_amount * 100
    print(f"{to_dec(perc)}% "
          f"({utils.kg_to_unit(company_amount, unit=VARS['COMPANY_WASTE_UNIT'])} {VARS['COMPANY_WASTE_UNIT']}) "
          f"afval geproduceerd door bedrijven")
    DATA['company_production'] = {
        "amount": to_dec(utils.kg_to_unit(
            company_amount,
            unit=VARS['COMPANY_WASTE_UNIT']
        )),
        "unit": VARS['COMPANY_WASTE_UNIT'],
        "pct": to_dec(perc)
    }


def top_company_production():
    print()
    lma = LMA.copy()
    lma['EC2'] = lma['EuralCode'].astype(str).str.zfill(6).str[:2]
    lma = lma[
        (lma[f"Herkomst_{VARS['LEVEL']}"] == VARS['AREA']) &
        (lma['EC2'] != '19')
        ]  # all waste produced except chapter 19
    groupby = [
        'Ontdoener',
        'Gewicht_KG'
    ]
    companies = lma[groupby].groupby(groupby[:-1]).sum().reset_index()
    companies = companies.sort_values(by=groupby[-1], ascending=False)
    bar_num = 100
    idx = int(len(companies) / bar_num)
    to_plot = companies[companies.index % idx == 0]
    ax = to_plot[:100].plot.bar(x=groupby[0], y=groupby[1], rot=0)
    plot.show()

    print('Enter company percent:')
    company_perc = int(input())
    idx = int(len(companies) * company_perc / 100)
    # companies[:idx].to_excel('greatest_producers.xlsx', index=False)
    company_amount = companies[:idx]['Gewicht_KG'].sum()
    amount_perc = company_amount / companies['Gewicht_KG'].sum() * 100
    print(f"{to_dec(amount_perc)}% "
          f"({utils.kg_to_unit(company_amount, unit=VARS['COMPANY_WASTE_UNIT'])} {VARS['COMPANY_WASTE_UNIT']}) "
          f"afval geproduceerd door "
          f"{company_perc}% van de bedrijven")
    DATA['top_company_production'] = {
        "amount": to_dec(utils.kg_to_unit(
            company_amount,
            unit=VARS['COMPANY_WASTE_UNIT'],
        )),
        "unit": VARS['COMPANY_WASTE_UNIT'],
        "pct": to_dec(amount_perc),
        "company_pct": company_perc
    }


def overview_highlights():
    # HIGHLIGHTS
    print('OVERVIEW HIGHLIGHTS')

    # total imported machine & apparatus
    total_imported_machine_apparatus()

    # total exported food
    total_exported_food()

    # waste produced by companies
    waste_produced_by_companies()

    # X% of waste produced by X% of companies
    top_company_production()


def run():
    ROLES = var.ROLES

    # start analysis
    print('HIGHLIGHTS ANALYSIS')
    print('VARIABLES:')
    for name, value in VARS.items():
        print(f'{name}={value}')

    # import postcodes
    postcodes = pd.read_csv(f"{VARS['INPUT_DIR']}/GEODATA/postcodes/{VARS['POSTCODES']}.csv", low_memory=False)
    postcodes['PC4'] = postcodes['PC4'].astype(str)
    gemeenten = postcodes[['Gemeente', 'Provincie']].drop_duplicates()
    area_gemeenten = gemeenten[gemeenten[f"{VARS['LEVEL']}"] == VARS['AREA']]['Gemeente'].to_list()
    print(f'AREA GEMEENTEN ({len(area_gemeenten)}): {sorted(area_gemeenten)}\n')

    # import province polygon
    polygon = utils.import_areas(level=VARS['LEVEL'])
    polygon = polygon[polygon['name'] == VARS['AREA']]
    assert len(polygon) == 1

    # import CBS household data
    global HOUSEHOLD
    if var.HOUSEHOLD_KG is not None:
        HOUSEHOLD = var.HOUSEHOLD_KG
    else:
        print('Import CBS household data...\n')
        HOUSEHOLD = import_household_data(areas=gemeenten)
        HOUSEHOLD = HOUSEHOLD.rename(columns={'Gebieden': 'Gemeente'})
        HOUSEHOLD = HOUSEHOLD[
            (HOUSEHOLD[VARS['LEVEL']] == VARS['AREA']) &
            (HOUSEHOLD['Perioden'] == VARS['YEAR'])
        ]
        HOUSEHOLD = cbs_primary_waste(HOUSEHOLD)

    # import CBS goods data
    global GOODS
    if len(VARS['COROPS']):
        print('Import CBS goods data... \n')
        path = f"{var.INPUT_DIR}/DATA/monitor_data/data/CBS"
        filename = f"{path}/{VARS['COROP_FILE']}.csv"
        GOODS = pd.read_csv(filename, low_memory=False)
        # stromen -> million kg
        GOODS['Gewicht_KG'] = GOODS['Brutogew'] * 10 ** 6  # mln kg -> kg
        GOODS['Gewicht_KG'] = GOODS['Gewicht_KG'].astype('int64')
        # filter by year & COROPS
        GOODS = GOODS[
            (GOODS['Jaar'] == VARS['YEAR']) &
            (GOODS['Regionaam'].isin(VARS['COROPS']))
        ]

    # import LMA data
    print('Import LMA Ontvangst...')
    global LMA
    typ = 'Ontvangst'
    path = f"{VARS['INPUT_DIR']}/{VARS['AREA_DIR']}/LMA/processed"
    filename = f"{path}/{typ.lower()}_{VARS['AREA'].lower()}_{VARS['YEAR']}_full.csv"
    LMA = pd.read_csv(filename, low_memory=False)
    # add areas to roles
    print('Add areas to roles...')
    source = ROLES[typ]['source']  # source role
    target = ROLES[typ]['target']  # target role
    for role in [source, target]:
        LMA = utils.add_areas(LMA,
                              areas=polygon,
                              role=role,
                              admin_level=VARS['LEVEL'])

    # compute highlights
    overview_highlights()

    return DATA





