import variables as var
import json
from src.analysis import (waste_trends,
                          benchmark,
                          eural_treemap)


DATA = {}

if __name__ == '__main__':
    # waste trends (production & processing)
    DATA = dict(**waste_trends.run())

    # eural treemap
    DATA['eural_treemap'] = eural_treemap.run()

    # benchmark sankey
    DATA['benchmark_sankey'] = benchmark.run()

    with open(f"{var.OUTPUT_DIR}/waste.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)

