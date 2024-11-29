# VARIABLES
INPUT_DIR = 'W:/Shared With Me/MASTER'
# INPUT_DIR = '/Users/rusnesileryte/Amazon WorkDocs Drive/My Documents/MASTER'
AREA = 'Zaanstad'
LEVEL = 'Gemeente'
AREA_DIR = f"PROJECTS/monitor/{LEVEL}{AREA}"
YEAR = 2022
DMI_YEARS = [_ for _ in range(2015, 2023)]
GOALS_YEARS = [_ for _ in range(2016, 2022)]
ACTIONS_YEARS = [_ for _ in range(2018, 2024)]
QUARTER = 4
COROP_FILE = f"Tabel Regionale stromen 2015-2022 {'provincie' if LEVEL == 'Provincie' else 'COROPplus'}"
COROPS = ['Zaanstreek']

POSTCODES = f'postcodes_per_gemeenten_{YEAR}'
OUTPUT_DIR = '../json'

# UNITS
unit = 'Mt' if LEVEL == 'Provincie' else 'kt'
UNITS = {
    'OVERVIEW': {
        'OVERVIEW_SANKEY': unit,
    },
    'MATERIALS': {
        'TRANSITION_AGENDAS': unit,
        'MATERIAL_TREE': 't'
    },
    'GOALS': {
        'NATIONAL_GOALS': 'Mt'
    },
    'HIGHLIGHTS': {
        'COMPANY_WASTE': unit
    }
}

ROLES = {
    'Ontvangst': {
        'source': 'Herkomst',
        'target': 'Verwerker',
        'activity': 'Ontdoener'
    },
    'Afgifte': {
        'source': 'EerstAfnemer',
        'target': 'Verwerker',
        'activity': 'EerstAfnemer'
    }
}

PREFIXES = {
    'Provincie': 'provincie',
    'Gemeente': 'gemeente',
    'Ontvangst': 'primair',
    'Afgifte': 'secondair',
    'Herkomst': 'production',
    'Verwerker': 'treatment',
}

# TREATMENT METHODS
# landfill = storten
# incineration = verbranden
# reuse = hergebruiken
# recycling = recyclen
# storage = bewaren

TREATMENT_METHODS = {
    'storten': ['G01', 'G02'],
    'verbranden': ['B04', 'F01', 'F02', 'F06', 'F07'],
    'hergebruiken': ['B01', 'B02', 'B03', 'B05'],
    'recyclen': ['C01', 'C02', 'C03', 'C04', 'D01',
                  'D02', 'D03', 'D04', 'D05', 'D06',
                  'E01', 'E02', 'E03', 'E04', 'E05',
                  'F03', 'F04', 'F05'],
    'bewaren': ['A01', 'A02']
}
