import variables as var
import json
from src.analysis import total_primary_waste, reuse_recycling


DATA = {}

if __name__ == '__main__':
    DATA['total_primary_waste'] = total_primary_waste.run()
    DATA = {**DATA, **reuse_recycling.run()}

    with open(f"{var.OUTPUT_DIR}/action_perspectives.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)