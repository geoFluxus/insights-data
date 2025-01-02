import variables as var
import json
from src.analysis import material_heatmap


DATA = {}

if __name__ == '__main__':
    DATA['raw_materials'] = material_heatmap.run()

    with open(f"{var.OUTPUT_DIR}/supply_security.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)