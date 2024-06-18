import streamlit as st
import pandas as pd
import plotly.express as px

# Required data
df_maintenance = pd.read_parquet("data/maintenance_combined.parquet")

def bike_station_status():

    unique_years = list(range(2019, 2024))
    unique_months = list(range(1, 13))
    unique_days = list(range(1, 32))
    unique_hours = list(range(24))
    # Define initial map view settings
    map_view = {'lat': 41.3999, 'lon': 2.17162, 'zoom': 11.5}
    with st.expander("Filters", expanded=True):
        selected_year = st.slider(
            "Select Year:",
            min_value=min(unique_years),
            max_value=max(unique_years),
            value=2020,
            step=1
        )
        selected_month = st.slider(
            "Select Month:",
            min_value=min(unique_months),
            max_value=max(unique_months),
            value=min(unique_months),
            step=1
        )
        selected_day = st.slider(
            "Select Day:",
            min_value=min(unique_days),
            max_value=max(unique_days),
            value=min(unique_days),
            step=1
        )
        selected_hour = st.slider(
            "Select Hour:",
            min_value=min(unique_hours),
            max_value=max(unique_hours),
            value=min(unique_hours),
            step=1
        )

    # Main content for map
    def update_map():
        try:
            selected_time = pd.Timestamp(year=selected_year, month=selected_month, day=selected_day, hour=selected_hour)
            filtered_data = df_maintenance[df_maintenance['datetime'] == selected_time]
        except ValueError:
            filtered_data = pd.DataFrame(columns=df_maintenance.columns)  # empty DataFrame if date is invalid

        fig = px.scatter_mapbox(
            filtered_data, 
            lat="lat", 
            lon="lon", 
            hover_name="name", 
            hover_data=["status", "num_bikes_available"],
            color='status',
            color_discrete_map={'IN_SERVICE': 'green', 'NOT_IN_SERVICE': 'red', "MAINTENANCE": "gray", "PLANNED": "blue"},
            zoom=map_view['zoom'], 
            height=600,
            center={"lat": map_view['lat'], "lon": map_view['lon']}
        )

        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        return fig

    # Display the map
    fig = update_map()
    st.plotly_chart(fig)