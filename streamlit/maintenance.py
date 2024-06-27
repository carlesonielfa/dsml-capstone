import streamlit as st
import pandas as pd
import plotly.express as px

# Required data
df_maintenance = pd.read_parquet("data/maintenance_combined.parquet")
# Add a new column to sum the times that status is not in service
df_maintenance["is_not_in_service"] = df_maintenance["status"] != "IN_SERVICE"

# Aggreagate data by day and station and sum service_int
df_data_agg = df_maintenance.groupby(["station_id", "year", "month", "day"]).agg({"is_not_in_service": "sum"}).reset_index()
df_data_agg = df_data_agg[df_data_agg["year"] >= 2020]
station_ids = df_data_agg['station_id'].unique()

def bike_station_status():

    st.header("Station Status Map")
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

def maintenance_calendar():
    st.header("Maintenance Calendar")
    st.write("We observed that something went wrong around 20th of April 2020, maybe a maintenance operation? Use the slider to explore the maintenance calendar for each station, can you see the pattern?")
    # Slider for station ids
    selected_station = st.slider("Select a station", min_value=min(station_ids), max_value=max(station_ids), value=1)

    st.write(f"#### Station {selected_station} Maintenance Calendar")
    # If we are missing data for the selected station, we will show an empty plot
    if selected_station not in station_ids:
        st.write(f"Station {selected_station} does not have any maintenance data.")
        return

    # Filter data for the selected station
    filtered_df = df_data_agg[df_data_agg['station_id'] == selected_station]
    cols = st.columns(2)
    for i,year in enumerate(range(2020, 2024)):
        with cols[i%2]:
            try:
                year_df = filtered_df[filtered_df["year"]==year]
                # Create a pivot table for calendar heatmap
                pivot_df = year_df.pivot_table(index="month", columns="day", values="is_not_in_service", fill_value=0)

                # Plot calendar heatmap
                fig = px.imshow(pivot_df,
                                labels=dict(x="Day", y="Month", color="Hours not in service"),
                                x=list(range(1, 32)),
                                y=list(range(1, 13)),
                                title=f"{year}")
                fig.update_layout(coloraxis_colorbar=dict(
                    orientation="h",
                    thickness=10,
                    xanchor="center",
                    len=1,
                    yanchor="top",
                    x=0.5,
                    y=1.02
                ))
                st.plotly_chart(fig)
            except ValueError:
                # Empty plot here
                fig = px.imshow(pd.DataFrame(columns=range(1, 32), index=range(1, 13)), title=f"{year}")
                # Don't show axis
                fig.update_xaxes(visible=False)
                fig.update_yaxes(visible=False)
                st.plotly_chart(fig)