import pandas as pd
import variables as var
import seaborn as sns
from seaborn.regression import _RegressionPlotter


PROJ_START = 2015
PROJ_END = 2030
DATA = {}


def regression(func, fig, *args, **kwargs):
    """ replication of seaborn function for regression analysis.
    This function produces identical graphs to the one provided by seaborn.
    In addition it prints out the lower and higher bounds of the confidence polygons.
    These bounds are used to define the confidence interval of the regression. """

    # column name
    col = args[1]

    # If color was a keyword argument, grab it here
    kw_color = kwargs.pop("color", None)

    # How we use the function depends on where it comes from
    func_module = str(getattr(func, "__module__", ""))

    # Iterate over the data subsets
    for (row_i, col_j, hue_k), data_ijk in fig.facet_data():
        # If this subset is null, move on
        if not data_ijk.values.size:
            continue

        # Get the current axis
        modify_state = not func_module.startswith("seaborn")
        ax = fig.facet_axis(row_i, col_j, modify_state)

        # Decide what color to plot with
        kwargs["color"] = fig._facet_color(hue_k, kw_color)

        # Insert the other hue aesthetics if appropriate
        for kw, val_list in fig.hue_kws.items():
            kwargs[kw] = val_list[hue_k]

        # Insert a label in the keyword arguments for the legend
        if fig._hue_var is not None:
            kwargs["label"] = fig.hue_names[hue_k]

        # Get the actual data we are going to plot with
        plot_data = data_ijk[list(args)]
        if fig._dropna:
            plot_data = plot_data.dropna()
        plot_args = [v for k, v in plot_data.items()]

        # Some matplotlib functions don't handle pandas objects correctly
        if func_module.startswith("matplotlib"):
            plot_args = [v.values for v in plot_args]

        # Draw the plot
        if str(func.__module__).startswith("seaborn"):
            plot_kwargs = kwargs.copy()
            semantics = ["x", "y", "hue", "size", "style"]
            for key, val in zip(semantics, plot_args):
                plot_kwargs[key] = val
            plot_args = []

        # regression
        plotter = _RegressionPlotter(*plot_args, **plot_kwargs)
        grid, yhat, err_bands = plotter.fit_regression(ax)

        return grid, yhat, err_bands


def get_sample(l):
    return [
        x for idx, x in enumerate(l)
        if idx % 10 == 0 or idx == len(l) - 1
    ]


def get_projected_y(x1, x2, y1, y2, x):
    m = (y2 - y1) / (x2 - x1)
    return y1 + m * (x - x1)


def visualise_per_province(data, indicator=None):
    for goal, goal_data in data.items():
        viz_data = goal_data.groupby(['Regionaam', 'Jaar']).sum().reset_index()

        sns.set()
        fig = sns.FacetGrid(data=viz_data, col='Regionaam', hue='Regionaam', aspect=0.5, height=5, col_wrap=6)
        fig.set(xlim=(PROJ_START, PROJ_END))

        grid, yhat, err_bands = regression(sns.regplot, fig, "Jaar", indicator, truncate=False)
        goal_value = viz_data[viz_data['Jaar'] == PROJ_START + 1][indicator].values[0] / 2
        proj_y = yhat[-1]
        if PROJ_END != var.PROJ_END:
            proj_y = get_projected_y(x1=PROJ_START,x2=PROJ_END,
                                     y1=yhat[0],y2=yhat[-1],
                                     x=var.PROJ_END)

        item = DATA.setdefault(indicator.lower(), {})
        item[goal.lower()] = {
            "points": {
                "period": viz_data["Jaar"].to_list(),
                "values": viz_data[indicator].to_list()
            },
            "line": {
                "x1": PROJ_START,
                "y1": yhat[0],
                "x2": var.PROJ_END,
                "y2": proj_y
            },
            "area": {
                "grid": get_sample(grid.tolist()),
                "upper_bound": get_sample(err_bands[0].tolist()),
                "lower_bound": get_sample(err_bands[1].tolist())
            },
            **({'goal': {
                "x1": PROJ_START,
                "y1": goal_value,
                "x2": PROJ_END,
                "y2": goal_value
            }} if goal == 'abiotic' and indicator in ['DMC'] else {})
        }


def run():
    # import dmi/dmc data
    data = {}
    for indicator in [
        'dmc', 'dmc_ab',
        'dmi', 'dmi_ab',
        'rmc', 'rmc_ab',
        'rmi', 'rmi_ab'
    ]:
        data[indicator] = pd.read_excel('../json/dmi_dmc.xlsx', sheet_name=indicator)

    indicators = {
        'DMC': {'total': data['dmc'], 'abiotic': data['dmc_ab']},
        'DMI': {'total': data['dmi'], 'abiotic': data['dmi_ab']},
        'RMC': {'total': data['rmc'], 'abiotic': data['rmc_ab']},
        'RMI': {'total': data['rmi'], 'abiotic': data['rmi_ab']}
    }
    for indicator, data in indicators.items():
        visualise_per_province(data, indicator=indicator)

    return {
        "level": var.PREFIXES[var.LEVEL],
        "period": var.YEAR,
        "name": var.AREA,
        "unit": "kt",
        "data": DATA,
    }



