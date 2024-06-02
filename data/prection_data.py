import pandas as pd


def get_prediction_data(prediction_raw_data_file_path):
    """
     Generate prediction data with lagged features from the raw prediction data.

     Args:
         prediction_raw_data_file_path (str): Path of the prediction_raw_data file.

     Returns:
         prediction_data (parquet): parquet with lagged features for each station.
     """

    prediction_raw_data = pd.read_parquet(prediction_raw_data_file_path)

    prediction_data = pd.DataFrame()
    for s in prediction_raw_data.station_id.unique():
        ctx = prediction_raw_data.loc[prediction_raw_data["station_id"] == s, :]
        ctx = ctx.sort_values(by=["year", "month", "day", "hour"],
                              ignore_index=True)
        for lag in range(1, 5):
            ctx.loc[:, f"ctx-{lag}"] = ctx.loc[:, "percentage_docks_available"].shift(lag)

        ctx = ctx.iloc[4::5]

        prediction_data = pd.concat([prediction_data, ctx], ignore_index=True)

    # Remove row where all lag variables are null
    prediction_data.dropna(subset=["ctx-1", "ctx-2", "ctx_3", "ctx-4"],
                           how="all",
                           inplace=True)

    prediction_data.to_parquet("prediction_data.parquet", index=False)


if __name__ == "__main__":
    get_prediction_data("prediction_data_raw.parquet")