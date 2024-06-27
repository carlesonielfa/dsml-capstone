# DSML-24 Capstone Project
**Bike Prediction**
Capstone Project Repo for Data Science and Machine Learning postgraduate course

## Repo description
### Data acquisition
1. `scripts/bicing_dowload_ios.py`: Script to download the Bicing data for UNIX computers
2. `scripts/bicing_dowload_windows.py`: Script to download the Bicing data for Windows computers
3. `notebooks/get_cleaned_data.ipynb`: Notebook to merge the downloaded csvs and apply some preprocessing
4. `scripts/bank_holidays.py`: Script to download the bank holiday information
5. `notebooks/Recolecta_data_meteocat.ipynb`: Notebook to download the weather data from Meteocat with the Meteocat API

### Data cleaning
1. `scripts/prediction_raw_data.py`: Filtering, processing and aggregation of the time series data with the station information. Prepares the data in the format required for the prediction model.

2. `notebooks/data_analysis_pl.ipynb`: Alternative to the previous script, but using Polars library for data processing.

### Modeling
- `notebooks/model_v1.ipynb`: Uses the data to train the model, evaluate it and generate the kaggle submission.

### Streamlit
To launch the streamlit app, run the following command in the terminal:

```bash
streamlit run scripts/bicing_app.py
```

### Station Maintenance
- `scripts/preprocess_maintenance.py`: Script that generates a parquet file with hourly maintenance information for each station from the downloaded csvs.
- `notebooks/visualize_maintenance.ipynb`: Notebook to visualize the maintenance information and to create and view the graphics that can be seen in the streamlit app.

### Time Series