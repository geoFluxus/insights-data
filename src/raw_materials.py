import variables as var
import json
from src.analysis import overview_materials, renewable_materials


DATA = {}

if __name__ == '__main__':
    DATA["overview_materials"] = overview_materials.run()
    DATA['renewable'] = renewable_materials.run()

    with open(f"{var.OUTPUT_DIR}/raw_materials.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)