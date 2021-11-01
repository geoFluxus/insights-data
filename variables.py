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
    'Provincie': 'province',
    'Gemeente': 'municipality',
    'Ontvangst': 'primary',
    'Afgifte': 'secondary',
}

TREATMENT_METHODS = {
    'landfill': ['G01', 'G02'],
    'incineration': ['B04', 'F01', 'F02', 'F06', 'F07'],
    'reuse': ['B01', 'B03', 'B05'],
    'recycling': ['C01', 'C02', 'C03', 'C04', 'D01',
                  'D02', 'D03', 'D04', 'D05', 'D06',
                  'E01', 'E02', 'E03', 'E04', 'E05',
                  'F03', 'F04'],
    'storage': ['A01', 'A02']
}
