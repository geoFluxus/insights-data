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
