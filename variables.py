# VARIABLES
INPUT_DIR = 'W:/Shared With Me/MASTER'
AREA = 'Enschede'
LEVEL = 'Gemeente'
AREA_DIR = f"PROJECTS/monitor/{LEVEL}{AREA}"
YEAR = 2023
DMI_YEARS = [_ for _ in range(2015, 2024)]
GOALS_YEARS = [_ for _ in range(2018, 2024)]
ACTIONS_YEARS = [_ for _ in range(2018, 2024)]
QUARTER = 4
COROP_FILE = f"Tabel Regionale stromen 2015-2023 " \
             f"{'provincie' if LEVEL == 'Provincie' else 'Coropplus'}"
COROPS = ['Twente']
HOUSEHOLD_KG = 61.09 * 10**6
EXCLUDE_HOUSEHOLD = True

POSTCODES = f'postcodes_per_gemeenten_{YEAR}'
OUTPUT_DIR = '../json'

# UNITS
unit = 'Mt' if LEVEL == 'Provincie' else 'kt'
UNITS = {
    'OVERVIEW': {
        'OVERVIEW_SANKEY': unit,
        'OVERVIEW_MATERIALS': 'kt',
        'HIGHLIGHTS': 'Mt',
        'OVERVIEW_USAGE': 'kt'
    },
    'MATERIALS': {
        'TRANSITION_AGENDAS': unit,
        'MATERIAL_TREE': 't',
        'HIGHLIGHTS': 'Mt'
    },
    'WASTE': {
        'TRENDS': 't',
        'BENCHMARK': 't',
        'EURAL_TREE': 't'
    },
    'NATIONAL_GOALS_UNIT': 'Mt'
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
    'verbranden': ['F01', 'F02'],
    'hergebruiken': ['B01', 'B02', 'B03', 'B05', 'C04'],
    'terugwinning': ['B04', 'F03', 'F04', 'F06', 'F07'],
    'recyclen': ['C01', 'C02', 'C03', 'D01',
                  'D02', 'D03', 'D04', 'D05', 'D06',
                  'E01', 'E02', 'E03', 'E04', 'E05',
                  'F05'],
    'bewaren': ['A01', 'A02']
}