import variables as var
import json
from src.analysis import (material_agendas_sankey,
                          overview_sankey,
                          overview_usage)


DATA = {}

if __name__ == '__main__':
    # waste trends (production & processing)
    DATA["overview_sankey"] = overview_sankey.run()
    DATA["overview_usage"] = overview_usage.run()
    DATA = dict({
        **DATA,
        **material_agendas_sankey.run()
    })

    with open(f"{var.OUTPUT_DIR}/materials.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)
