import pandas as pd
import numpy as np


def remove_datetime_duplicates(df):
    l = []
    for _, group in df.groupby("station_id"):
        station = group.drop_duplicates(subset="datetime", keep="first")
        l.append(station)

    result = pd.concat(l, ignore_index=True)

    return result


def get_prediction_raw_data(clean_data_file_path, station_information_file_path):
    clean_data = pd.read_parquet(clean_data_file_path)
    station_information = pd.read_csv(station_information_file_path)

    # Add capacity for each station_id
    merge = clean_data.merge(station_information[["station_id", "capacity"]],
                             on="station_id",
                             how="inner")

    # Create percentage docks available
    merge["percentage_docks_available"] = merge["num_docks_available"] / merge["capacity"]

    # Filter out records where "num_docks_available" > "capacity"
    merge = merge[merge["num_docks_available"] <= merge["capacity"]]

    # Filter out records before 2020
    merge = merge[merge["year"] >= 2020]

    # Create datetime column
    merge['datetime'] = pd.to_datetime(merge[['year', 'month', 'day', 'hour']].astype(str).agg('-'.join, axis=1),
                                       format='%Y-%m-%d-%H')

    merge = merge[["station_id", "year", "month", "day",
                   "hour", "num_docks_available", "capacity",
                   "percentage_docks_available", "datetime"]]

    # Remove duplicates
    merge = remove_datetime_duplicates(merge)

    # Ensure that all the station_id have all dates from min_data to max_data
    list_df_station = []
    for s in merge.station_id.unique():
        df_station = merge[merge["station_id"] == s]
        datetime_available = df_station["datetime"].unique()
        min_data = min(datetime_available)
        max_data = max(datetime_available)

        complete_datetime_range = pd.date_range(start=min_data, end=max_data, freq='H')
        missing_datetime = list(set(complete_datetime_range) - set(datetime_available))

        rows_to_add = []
        for d in missing_datetime:
            rows_to_add.append({
                "station_id": s,
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "hour": d.hour,
                "num_docks_available": np.nan,
                "capacity": np.nan,
                "datetime": d})

        new_rows = pd.DataFrame(rows_to_add)

        df_station = pd.concat([df_station, new_rows], ignore_index=True)

        list_df_station.append(df_station)

    prediction_raw_data = pd.concat(list_df_station, ignore_index=True)

    #  Export dataFrame
    prediction_raw_data.to_parquet("prediction_raw_data.parquet", index=False)


if __name__ == "__main__":
    get_prediction_raw_data("cleaned_data.parquet", "station_information.csv")
