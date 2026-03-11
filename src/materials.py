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
    highlights = DATA['highlights'] = {}

    # highest stream
    data = DATA["overview_sankey"]["flows"]
    max_key = max(data, key=lambda k: data[k]["values"][0])
    highlights['highest_stream'] = {
        'name': max_key.replace("_", " ").capitalize()
    }

    # highest usage
    values = DATA['overview_usage']['values']
    max_key = max(values, key=lambda k: sum(values[k]))
    highlights['highest_usage'] = {
        'name': max_key.replace("_", " ").capitalize()
    }

    # highest good & waste
    data = DATA['material_table'][0]["data"]
    parents = {item["parent"] for item in data if item["parent"] is not None}
    leaf_nodes = [item for item in data if item["material"] not in parents]
    max_waste = max(leaf_nodes, key=lambda x: x["amount_waste"])['material']
    max_goods = max(leaf_nodes, key=lambda x: x["amount_goods"])['material']
    highlights['highest_goods'] = {
        'name': max_goods.replace("_", " ").capitalize()
    }
    highlights['highest_waste'] = {
        'name': max_waste.replace("_", " ").capitalize()
    }

    with open(f"{var.OUTPUT_DIR}/materials.json", 'w') as outfile:
        from src import _make_iterencode
        json.encoder._make_iterencode = _make_iterencode._make_iterencode
        indent = (2, None)
        json.dump(DATA, outfile, indent=indent)
