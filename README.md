# Insights data
An analysis tool for monitorce.nl

## Local Installation
### Clone repo
If you have Git or any other related software ([GitHub Desktop](https://desktop.github.com/), [GitKraken](https://www.gitkraken.com/) etc.), clone the repo in your computer with the command:

```(git clone) https://github.com/geoFluxus/insights-data.git```

### Python env 
Make sure that [Python3](https://www.python.org/downloads/) is installed in your computer. To check that, open a terminal and insert the command:

```python --version```

If the command is recognized, the currentrly installed version of Python should appear in your screen. Once you have verified that Python3 is properly installed, enter the following command to download the necessary dependencies:

```pip3 install -r requirements.txt```

(If you wish to set up a Python virtual environment instead, please check the instructions [here](https://docs.python.org/3/tutorial/venv.html)).

The major analysis tool we use for spatial data is [geopandas](https://geopandas.org/en/stable/). If the installation through requirements.txt fails, follow the instructions below:
- **For Windows**: To install geopandas, visit this [site](https://www.lfd.uci.edu/~gohlke/pythonlibs/) and download **in the given order** the windows binaries
  for [GDAL](https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal), [pyproj](https://www.lfd.uci.edu/~gohlke/pythonlibs/#pyproj),
  [Fiona](https://www.lfd.uci.edu/~gohlke/pythonlibs/#fiona), [Shapely](https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely),
  [geopandas](https://www.lfd.uci.edu/~gohlke/pythonlibs/#geopandas) and [Rtree](https://www.lfd.uci.edu/~gohlke/pythonlibs/#rtree).
  Make sure you choose the wheel files that match your architecture (win32, win_amd64) and Python version (cp39, cp38). To install each of the binaries,
  use the command: ```pip install path/to/binary```. In the end, do not forget to install any of the packages in the requirements.txt
  not mentioned here.


## Analysis
### WorkDocs Drive
Before running any analysis, make sure you have access to the geoFluxus site on AWS WorkDocs and you have install [WorkDocs Drive](https://docs.aws.amazon.com/workdocs/latest/userguide/drive_install.html).
For any issues on installing WorkDocs Drive on Windows, please check [here](https://aws.amazon.com/premiumsupport/knowledge-center/workdocs-authenticating-user/).

### Scripts
The analysis consists of the following scripts:
- **overview.py**: Exports data for the *Overzicht* page to **overview.json**
<br/><br/>
- **matetials.py**: Exports data for the *Materialen* page to **materials.json**
<br/><br/>
- **goals.py**: Exports data for the *Nationale doelen* section of the *Doelen en vooruitgang* to **household.json**
<br/><br/>
- **networkmap.py**: Exports data for the **Emissies* section of the *Doelen en vooruitgang* to **overview_co2_network.json**.
  Provides also data on the section highlights.
<br/><br/>
- **actions.py**: Exports data for the *Handelingsperspectieven* page to **actions.json**
<br/><br/>
- **flowmaps.py**: Exports data for the flowmaps of the *Handelingsperspectieven* page to **overview_activities_flowmap.json** 
  and **overview_processes_flowmap.json**
<br/><br/>
- **highlights.py**: Produces data for the highlights in the *Overzicht* & *Materialen* pages


| Script     | Page                                     | Export file                                                          |
|------------|------------------------------------------|----------------------------------------------------------------------|
| overview   | Overzicht                                | overview.json                                                        |
| materials  | Materialen                               | materials.json                                                       |
| goals      | Doelen en vooruitgang (Nationale Doelen) | household.json                                                       |
| networkmap | Doelen en vooruitgang (Emissies)         | overview_co2_network.json                                            |
| actions    | Handelingsperspectieven                  | actions.json                                                         |
| flowmaps   | Handelingsperspectieven (flowmaps)       | overview_activities_flowmap.json<br/>overview_processes_flowmap.json |
| highlights | Overzicht & Materialen (highlights)      | -                                                                    |

### Variables
The analysis variables for the scripts can be found in **variables.py**:
- **INPUT_DIR**: The data folder on WorkDocs Drive
- **AREA**: The name of the area in study
- **LEVEL**: The administrative level of the area in study (Gemeente/Provincie)
- **YEAR**: The main year of analysis
- **GOALS_YEARS**: The list of years for the goals analysis
- **ACTIONS_YEARS**: The list of year for the actions analysis
- **QUARTER**: The quarter in interest for the actions analysis
- **COROPS**: The list of [COROPS](https://en.wikipedia.org/wiki/COROP) regions related to the area in study
- **POSTCODES**: A list of postcodes per municipalities in NL
- **OUTPUT_DIR**: The folder to export analysis data  

| Variable      | Description                                                                                    |
|---------------|------------------------------------------------------------------------------------------------|
| INPUT_DIR     | The data folder on WorkDocs Drive                                                              |
| AREA          | The name of the area in study                                                                  |
| LEVEL         | The administrative level of the area in study (Gemeente/Provincie)                             |
| YEAR          | The main year of analysis                                                                      |
| GOALS_YEARS   | The list of years for the goals analysis                                                       |
| ACTIONS_YEARS | The list of year for the actions analysis                                                      |
| QUARTER       | The quarter in interest for the actions analysis                                               |
| COROPS        | The list of [COROPS](https://en.wikipedia.org/wiki/COROP) regions related to the area in study |
| POSTCODES     | A list of postcodes per municipalities in NL                                                   |
| OUTPUT_DIR    | The folder to export analysis data                                                             |