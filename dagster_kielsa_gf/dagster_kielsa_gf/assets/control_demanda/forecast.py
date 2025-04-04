import polars as pl
import pandas as pd
import matplotlib.pyplot as plt
from statsforecast import StatsForecast
from statsforecast.models import Naive, SeasonalNaive, AutoARIMA
from sklearn.metrics import mean_absolute_error
from coreforecast.seasonal import find_season_length


def plot_best_forecast():
    # Generate an hourly time series for 4 weeks using Polars.
    df = pl.DataFrame(
        {
            "ds": pl.datetime_range(
                start=pl.datetime(2023, 1, 22),
                end=pl.datetime(2023, 2, 19),
                interval="1h",
                eager=True,
            ),
            "unique_id": "test_series",  # Required for StatsForecast.
        }
    )

    # Set a constant base level.
    df = df.with_columns(pl.lit(10).alias("y"))

    # Inject a level shift (e.g., add 5 units) on every Monday at 13:00.
    df = df.with_columns(
        ((pl.col("ds").dt.weekday() == 0) & (pl.col("ds").dt.hour() == 13)).alias(
            "is_outlier"
        )
    )
    df = df.with_columns(
        pl.when(pl.col("is_outlier"))
        .then(pl.col("y") + 5)
        .otherwise(pl.col("y"))
        .alias("y")
    ).drop("is_outlier")

    # Split the dataset into training and test sets.
    # Training: data before 2023-01-22, Test: data on/after 2023-01-22.
    train = df.filter(pl.col("ds") < pl.datetime(2023, 1, 22))
    test = df.filter(pl.col("ds") >= pl.datetime(2023, 1, 22))

    # Convert to Pandas DataFrames for compatibility with StatsForecast.
    train_pd = train.to_pandas()
    test_pd = test.to_pandas()

    # Define forecasting models.
    models = [
        Naive(),
        SeasonalNaive(find_season_length(train_pd["y"], max_season_length=12)),  # type: ignore
        AutoARIMA(),
    ]

    # Create a StatsForecast instance using the training data.
    sf = StatsForecast(df=train_pd, models=models, freq="h", n_jobs=1)  # type: ignore

    # Forecast for the same number of steps as the test set.
    steps = len(test_pd)
    forecast_df = sf.forecast(h=steps)  # type: ignore

    # If forecast_df does not include a 'ds' column, create one based on training data's last timestamp.
    if "ds" not in forecast_df.columns:
        last_date = pd.to_datetime(train_pd["ds"]).max()
        forecast_dates = pd.date_range(
            start=last_date + pd.Timedelta(hours=1), periods=steps, freq="H"
        )
        forecast_df["ds"] = forecast_dates

    # Evaluate the performance using Mean Absolute Error (MAE) on the test period.
    model_names = ["Naive", "SeasonalNaive", "AutoARIMA"]
    errors = {}
    for model in model_names:
        forecast_values = forecast_df[model].values
        actual_values = test_pd["y"].values
        errors[model] = mean_absolute_error(actual_values, forecast_values)  # type: ignore

    # Select the best model (lowest MAE).
    best_model = min(errors, key=errors.get)  # type: ignore
    print("MAE Errors:", errors)
    print("Best model based on MAE:", best_model)

    # Prepare forecast data from the best model.
    best_forecast = forecast_df[["ds", best_model]].copy()
    best_forecast["ds"] = pd.to_datetime(best_forecast["ds"])

    # Ensure the ds columns in training and test data are datetime.
    train_pd["ds"] = pd.to_datetime(train_pd["ds"])
    test_pd["ds"] = pd.to_datetime(test_pd["ds"])

    # Plot the training data, test data, and best model forecast.
    plt.figure(figsize=(12, 6))
    plt.plot(train_pd["ds"], train_pd["y"], label="Train", color="blue")
    plt.plot(test_pd["ds"], test_pd["y"], label="Test", color="green")
    plt.plot(
        best_forecast["ds"],
        best_forecast[best_model],
        label=f"Forecast ({best_model})",
        color="orange",
        linestyle="--",
    )

    # Highlight the outlier points (every Monday at 13:00).
    # For training data.
    train_outliers = train_pd.loc[
        train_pd["ds"].apply(lambda x: x.weekday() == 0 and x.hour == 13)
    ]
    plt.scatter(
        train_outliers["ds"], train_outliers["y"], color="red", label="Outliers (Train)"
    )

    # For test data.
    test_outliers = test_pd.loc[
        test_pd["ds"].apply(lambda x: x.weekday() == 0 and x.hour == 13)
    ]
    plt.scatter(
        test_outliers["ds"], test_outliers["y"], color="purple", label="Outliers (Test)"
    )

    plt.xlabel("Time")
    plt.ylabel("Value")
    plt.title("Forecast Comparison with Best Model and Outlier Highlight")
    plt.legend()
    plt.grid(True)

    # If running in an interactive environment (e.g., Jupyter), plt.show() should work.
    # Otherwise, you might consider saving the figure with plt.savefig('forecast.png')
    plt.savefig("fig.png")


if __name__ == "__main__":
    plot_best_forecast()
