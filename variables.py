# VARIABLES
INPUT_DIR = 'W:/My Documents/VASILIS'
AREA = 'Utrecht'
LEVEL = 'Provincie'
YEAR = 2019
GOALS_YEARS = [2016, 2017, 2018, 2019]
ACTIONS_YEARS = [2016, 2017, 2018, 2019, 2020, 2021]
QUARTER = 2
COROPS = [
    'Utrecht'
]
POSTCODES = 'postcodes_per_gemeenten_2021'
OUTPUT_DIR = './json'

# UNITS
UNITS = {
    'OVERVIEW': {
        'OVERVIEW_SANKEY': 'Mt',
        'SUPPLY_CHAINS': 'kt'
    },
    'MATERIALS': {
        'TRANSITION_AGENDAS': 'Mt',
        'MATERIAL_TREE': 'Mt'
    },
    'GOALS': {
        'NATIONAL_GOALS': 'Mt'
    },
    'HIGHLIGHTS': {
        'COMPANY_WASTE': 'Mt'
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
    'hergebruiken': ['B01', 'B03', 'B05'],
    'recyclen': ['C01', 'C02', 'C03', 'C04', 'D01',
                  'D02', 'D03', 'D04', 'D05', 'D06',
                  'E01', 'E02', 'E03', 'E04', 'E05',
                  'F03', 'F04'],
    'bewaren': ['A01', 'A02']
}
