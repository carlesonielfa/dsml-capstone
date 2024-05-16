import pandas as pd
import pyarrow.parquet as pq


def get_prediction_data(clean_data_file_name, station_information_file_name):
    clean_data = pd.read_parquet(clean_data_file_name)
    station_information = pd.read_csv(station_information_file_name)

    merge = clean_data.merge(station_information[["station_id", "capacity"]],
                             on="station_id",
                             how="left")

    merge["percentage_docks_available"] = merge["num_docks_available"] / merge["capacity"]

    prediction_data_raw = merge[["station_id", "year", "month", "day",
                                 "hour", "num_docks_available", "capacity", "percentage_docks_available"]]

    prediction_data_raw.to_parquet("prediction_data_raw.parquet", index=False)

    merge = merge[["station_id", "year", "month", "day",
                   "hour", "percentage_docks_available"]]

    prediction_data = pd.DataFrame()

    for s in merge.station_id.unique():
        ctx = merge.loc[merge["station_id"] == s, :]
        ctx = ctx.sort_values(by=["year", "month", "day", "hour"],
                              ignore_index=True)

        for lag in range(1, 5):
            ctx.loc[:, f"ctx-{lag}"] = ctx.loc[:, "percentage_docks_available"].shift(lag)

        ctx = ctx.iloc[4::5]

        prediction_data = pd.concat([prediction_data, ctx], ignore_index=True)

    prediction_data.to_parquet("prediction_data.parquet", index=False)

if __name__ == "__main__":
    get_prediction_data("cleaned_data.parquet", "station_information.csv")
