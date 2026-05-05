# VARIABLES
INPUT_DIR = 'C:/Users/vasil/geoFluxus Dropbox/geoFluxus'
AREA = 'Deventer'
LEVEL = 'Gemeente'
AREA_DIR = f"Monitors/{LEVEL}{AREA}"
YEAR = 2023
PROJ_END = 2030
DMI_YEARS = [_ for _ in range(2015, 2024)]
GOALS_YEARS = [_ for _ in range(2018, 2024)]
ACTIONS_YEARS = [_ for _ in range(2018, 2024)]
QUARTER = 4
COROP_FILE = f"Tabel Regionale stromen 2015-2023 " \
             f"{'provincie' if LEVEL == 'Provincie' else 'Coropplus'}"
COROPS = ['Zuidwest-Overijssel']
HOUSEHOLD_KG = 37.22 * 10**6
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

PRODUCTGROEPEN = {
    'VoedingLandbouw': 'Voeding en landbouw',
    'HoutPapier': 'Hout en Papier',
    # 'Fossiel': 'Fossiele Energiedragers',
    'Metaalproducten': 'Basismetalen en verwerkte metaalproducten',
    'ChemieFarmacie': 'Chemische & farmaceutische producten',
    'Bouw': 'B&U en GWW',
    'Textiel': 'Textiel',
    'KunststoffenRubber': 'Kunststoffen en Rubber',
    "Voertuigen": "Voertuigen exclusief auto's",
    'Consumentenelektronica': 'Consumentenelektronica',
    'Apparaten': 'Complexe elektrische en elektronische apparaten',
    'Machines': 'Machines',
    'Auto': "Auto's",
    'Meubels': 'Meubels',
    'Overig': 'Overig',
    'Verpakkingen': 'Verpakkingen',
    'Restafval': 'Restafval',
    "Batterijen en Accu's": "Batterijen"
}

AFVAL_PRODUCTS = [
    'Verpakkingen',
    'Restafval',
    "Batterijen"
]