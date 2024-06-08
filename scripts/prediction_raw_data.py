import pandas as pd
import numpy as np
import pyarrow.parquet as pq


def remove_datetime_duplicates(df):
    l = []
    for _, group in df.groupby("station_id"):
        station = group.drop_duplicates(subset="datetime", keep="first")
        l.append(station)

    result = pd.concat(l, ignore_index=True)

    return result


def read_cleaned_data_parquet(filename: str) -> pd.DataFrame:
    table = pq.read_table(filename)
    cleaned_data = table.to_pandas()
    return cleaned_data


def get_prediction_raw_data(clean_data_file_path,
                            station_information_file_path,
                            metadata_sample_submission_path,
                            bank_holidays_bcn_path):
    cleaned_data = read_cleaned_data_parquet(clean_data_file_path)

    station_information = pd.read_csv(station_information_file_path)
    metadata_sample_submission = pd.read_csv(metadata_sample_submission_path)
    bank_holidays_bcn = pd.read_csv(bank_holidays_bcn_path)

    # Add capacity for each station_id
    merge = cleaned_data.merge(station_information[["station_id", "capacity", "lat", "lon", "altitude", "post_code"]],
                               on="station_id",
                               how="inner")

    # Filter out station_id not in metadata_sample_submission
    metadata_station_id_list = metadata_sample_submission["station_id"].unique()
    merge = merge[merge["station_id"].isin(metadata_station_id_list)]

    # Ensure that "num_docks_available" is not > "capacity"
    merge.loc[merge["num_docks_available"] > merge["capacity"], "num_docks_available"] = merge.loc[
        merge["num_docks_available"] > merge["capacity"], "capacity"]

    # Create percentage docks available
    merge["percentage_docks_available"] = merge["num_docks_available"] / merge["capacity"]

    # Filter out records before 2020
    merge = merge[merge["year"] >= 2020]

    # Create datetime column
    merge['datetime'] = pd.to_datetime(merge[['year', 'month', 'day', 'hour']].astype(str).agg('-'.join, axis=1),
                                       format='%Y-%m-%d-%H')

    merge = merge[["station_id", "lat", "lon", "altitude", "post_code", "year", "month", "day",
                   "hour", "num_docks_available", "capacity",
                   "percentage_docks_available", "datetime"]]

    # Remove duplicates
    merge = remove_datetime_duplicates(merge)

    # Ensure that all the station_id have all dates from min_data to max_data
    list_df_station = []
    for s in merge.station_id.unique():
        df_station = merge[merge["station_id"] == s]
        lat = df_station["lat"].iloc[0]
        lon = df_station["lon"].iloc[0]
        altitude = df_station["altitude"].iloc[0]
        post_code = df_station["post_code"].iloc[0]
        capacity = df_station["capacity"].iloc[0]
        datetime_available = df_station["datetime"].unique()
        min_data = min(datetime_available)
        max_data = max(datetime_available)

        complete_datetime_range = pd.date_range(start=min_data, end=max_data, freq='h')
        missing_datetime = list(set(complete_datetime_range) - set(datetime_available))

        rows_to_add = []
        for d in missing_datetime:
            rows_to_add.append({
                "station_id": s,
                "lat": lat,
                "lon": lon,
                "altitude": altitude,
                "post_code": post_code,
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "hour": d.hour,
                "num_docks_available": np.nan,
                "capacity": capacity,
                "datetime": d})

        new_rows = pd.DataFrame(rows_to_add)

        df_station = pd.concat([df_station, new_rows], ignore_index=True)

        list_df_station.append(df_station)

    prediction_raw_data = pd.concat(list_df_station, ignore_index=True)

    # Add bank holidays
    prediction_raw_data["date"] = prediction_raw_data["datetime"].dt.date
    bank_holidays_bcn["holiday_date"] = pd.to_datetime(bank_holidays_bcn["holiday_date"]).dt.date

    holiday_dates_list = bank_holidays_bcn["holiday_date"].unique()
    prediction_raw_data["is_holidays"] = prediction_raw_data["date"].isin(holiday_dates_list)

    #  Export dataFrame
    prediction_raw_data.to_parquet("../data/prediction_raw_data.parquet", index=False)


if __name__ == "__main__":
    get_prediction_raw_data("../data/cleaned_data.parquet",
                            "../data/station_information.csv",
                            metadata_sample_submission_path="../data/metadata_sample_submission.csv",
                            bank_holidays_bcn_path="../data/bank_holidays_bcn.csv")
