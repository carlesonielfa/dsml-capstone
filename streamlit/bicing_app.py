import streamlit as st
from map_view import bike_station_status

# Title
st.title("BICING APP")

# Subtitle
st.write("## Capstone Project")

# Main content
st.write(
    """
    Welcome to the BICING APP! This app is designed to display the results of our capstone project.
    
    Our Team form by:
    - Pol
    - Mario
    - Quim Bassa
    - Carles Onielfa
    - Jacint

    ### Project Overview
    In this project, participants were challenged to develop a predictive model to forecast the availability of docked bikes at various bike-sharing stations in Barcelona. Bike sharing is a popular mode of transportation in many urban areas, and accurate predictions of bike availability can help riders plan their commutes and minimize wait times.
    
    Given a dataset with historical information of the stations, they were tasked with developing models that can accurately forecast the availability of docks at different times and locations.

    """)

# Bike prediction

st.write(
    """
    ### Prediction Task

    #### Data Preprocessing
    TODO: Write this out
    * Read each file
    * Filter to only have in service
    * Last reported to datetime
    * Remove nulls in station_id and last_reported

    #### Dataset Creation
    TODO: Write this out
    * Remove stations that don’t have metadata
    * Create percentage docks available
    * Merge Tables station information + Metadata + Bank Holidays BCN + Meteo Data
    * Filter out records before 2020
    * Keep only necessary columns for prediction
    * Remove duplicates


    #### Prediction Models
    TODO: Write this out
    * Days and hours encoded by sinus and cosinus function
    * Column encoding
    * XGBoost

    """
)

# Case study
st.write(
    """
    ### Case Study Task
    """
)

# Maintenance
st.write(
    """
    #### Maintenance Analysis
    """
)
# Map
bike_station_status()

# Temporal series analysis
st.write(
    """
    #### Temporal Series Analysis
    """
)
