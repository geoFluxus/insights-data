import variables as var
import json
from src.analysis import environmental_cost


DATA = {}

if __name__ == '__main__':
    DATA = environmental_cost.run()

    with open(f"{var.OUTPUT_DIR}/impact.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)