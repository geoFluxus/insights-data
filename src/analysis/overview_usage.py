import variables as var
import pandas as pd
from src.analysis import utils


def run(on_agendas=False):
    unit = var.UNITS['OVERVIEW']['OVERVIEW_USAGE']

    # Load aggregated dataset
    file_path = f"{var.OUTPUT_DIR}/all_data.xlsx"

    df = pd.read_excel(
        file_path,
        sheet_name="NON_FE"
    )

    # Filter year, region and usage
    df = df[
        (df["Jaar"] == var.YEAR) &
        (df["Regionaam"].isin(var.COROPS)) &
        (df["Gebruiksgroep_naam"] != "Niet van toepassing")
    ].copy()

    # Add product group classification
    classif_path = (
        f"{var.INPUT_DIR}/Database_LockedFiles/"
        f"DATA/ontology/npce_productgroepen.xlsx"
    )

    productgroepen = pd.read_excel(classif_path)

    df = utils.add_classification(
        df,
        productgroepen,
        name="productgroepen",
        left_on="cbs",
        right_on="cbs"
    )

    stromen = [
        "Aanbod_eigen_regio",
        "Invoer_nationaal",
        "Invoer_internationaal",
    ]

    usages = [
        "Consumptie huishoudens",
        "Dienstverlening bedrijven",
        "Productie goederen",
        "Overheid",
        "Investeringen vaste activa",
        "Verandering voorraden",
    ]

    values = {}

    for usage in usages:
        values[usage] = []

        usage_df = df[
            df["Gebruiksgroep_naam"] == usage
        ].copy()

        for stroom in stromen:
            stroom_df = usage_df.copy()

            # NON_FE stream columns are in mln kg.
            # get_classification_graphs expects Gewicht_KG.
            stroom_df["Gewicht_KG"] = (
                pd.to_numeric(
                    stroom_df[stroom],
                    errors="coerce"
                )
                .fillna(0)
                * 10 ** 6
            )

            if on_agendas:
                graph = utils.get_classification_graphs(
                    stroom_df,
                    area=var.COROPS,
                    klass="productgroepen",
                    unit=unit
                )

                values[usage].append({
                    k: v
                    for k, v in graph.items()
                    if k in [
                        "productgroepen",
                        "values"
                    ]
                })

            else:
                value = utils.kg_to_unit(
                    stroom_df["Gewicht_KG"].sum(),
                    unit=unit
                )

                values[usage].append(value)

    return {
        "level": "COROP",
        "name": var.COROPS[0],
        "period": var.YEAR,
        "type": "goederen",
        "unit": unit,
        "usage": [
            stroom.replace("_", " ")
            for stroom in stromen
        ],
        "values": values
    }