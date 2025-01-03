import variables as var
import json
from src.analysis import (material_agendas_sankey,)


DATA = {}

if __name__ == '__main__':
    # waste trends (production & processing)
    DATA = dict(**material_agendas_sankey.run())

    with open(f"{var.OUTPUT_DIR}/materials.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)
