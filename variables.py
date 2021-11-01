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