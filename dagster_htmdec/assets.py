import io
from typing import Tuple

from dagster import (
    AssetOut,
    Definitions,
    EnvVar,
    FilesystemIOManager,
    asset,
    multi_asset,
)
from pandas import DataFrame, get_dummies, read_csv, read_html
from sklearn.linear_model import LinearRegression as Regression

from .io_manager import ConfigurableGirderIOManager


@asset
def country_stats() -> DataFrame:
    df = read_html(
        "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"
    )[0]
    df.columns = [
        "country",
        "continent",
        "subregion",
        "pop_20220701",
        "pop_20230701",
        "pop_change",
    ]
    df["pop_change"] = df["pop_change"].str.replace("−", "-")
    df["pop_change"] = df["pop_change"].str.rstrip("%").astype("float")
    return df


@asset
def change_model(country_stats: DataFrame) -> Regression:
    data = country_stats.dropna(subset=["pop_change"])
    dummies = get_dummies(data[["continent"]])
    return Regression().fit(dummies, data["pop_change"])


@multi_asset(
    outs={
        "continent_stats_df": AssetOut(io_manager_key="fs_io_manager"),
        "continent_stats_csv": AssetOut(io_manager_key="girder_io_manager"),
    }
)
def continent_stats(
    country_stats: DataFrame, change_model: Regression
) -> Tuple[DataFrame, io.BytesIO]:
    result = country_stats.groupby("continent").sum()
    result["pop_change_factor"] = change_model.coef_
    fp = io.BytesIO()
    result.to_csv(fp)
    fp.seek(0)
    return result, fp


@asset(io_manager_key="girder_io_manager")
def stats_read_from_girder(continent_stats_csv: io.BytesIO) -> None:
    df = read_csv(continent_stats_csv)
    print(df.head())


defs = Definitions(
    assets=[country_stats, change_model, continent_stats, stats_read_from_girder],
    resources={
        "fs_io_manager": FilesystemIOManager(),
        "girder_io_manager": ConfigurableGirderIOManager(
            api_key=EnvVar("GIRDER_API_KEY"),
            api_url=EnvVar("GIRDER_API_URL"),
            folder_id=EnvVar("GIRDER_FOLDER_ID"),
        ),
    },
)
