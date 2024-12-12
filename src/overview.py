from analysis import (overview_sankey,
                      overview_materials,
                      overview_highlights)
import json
import variables as var


DATA = {}

if __name__ == '__main__':
    # DATA["overview_sankey"] = overview_sankey.run()
    # DATA["overview_materials"] = overview_materials.run()
    DATA["overview_highlights"] = overview_highlights.run()

    with open(f"{var.OUTPUT_DIR}/overview.json", 'w') as outfile:
        from src import _make_iterencode

        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)