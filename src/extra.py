import json
import variables as var
from src.analysis import (overview_materials,
                          overview_sankey,
                          overview_usage)
from src.analysis.utrecht import transition_agendas


DATA = {}

if __name__ == "__main__":
    # overview_materials.run()
    DATA['sankey'] = overview_sankey.run(on_agendas=True)
    DATA['usage'] = overview_usage.run(on_agendas=True)
    DATA['agendas'] = transition_agendas.run()

    with open(f"{var.OUTPUT_DIR}/extra.json", 'w') as outfile:
        from src import _make_iterencode

        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)