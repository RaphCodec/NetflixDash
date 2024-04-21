import pandas as pd
from pandas.api.types import is_string_dtype
from numpy import nan, int64
from loguru import logger
from icecream import ic
import sqlite3
import sys
import json

pd.set_option("display.max_columns", None)


def map_values_to_ids(value, table, col):
    if value is None:
        return None
    else:
        try:
            return table.set_index(col).loc[value][f"{col}_id"]
        except KeyError:
            return None


def normalize(fact_table, primary_key, fact_name, exclude=[], exploders=[]) -> dict:
    normalized_dfs = {}
    for col in fact_table.columns:
        if col not in exclude and col in exploders and col != primary_key:
            table = pd.DataFrame(data={col: fact_table.explode(col)[col].unique()})
            table.replace("None", None, regex=True, inplace=True)
            table.dropna(inplace=True)
            table.reset_index(inplace=True, drop=True)
            table.insert(loc=0, column=f"{col}_id", value=table.index + 1)
            normalized_dfs[col] = table
            mid_table = (
                fact_table[[primary_key, col]]
                .explode(col)
                .replace("None", None)
                .dropna()
            )
            mid_table[col] = mid_table[col].map(
                table.set_index(col)[f"{col}_id"].to_dict()
            )
            ic(mid_table)
            fact_table[col] = fact_table[col].apply(
                lambda x: [map_values_to_ids(val, table, col) for val in x]
            )
            ic(fact_table[col])
        elif (
            is_string_dtype(fact_table[col]) == True
            and col not in exclude
            and fact_table[col].duplicated().any() == True
            and col != primary_key
        ):
            table = pd.DataFrame(data={col: fact_table[col].unique()})
            table.insert(loc=0, column=f"{col}_id", value=table.index + 1)
            normalized_dfs[col] = table
            # replacing fact table valeus with look up values
            fact_table[col] = fact_table[col].map(
                table.set_index(col)[f"{col}_id"].to_dict()
            )

    if fact_name:
        normalized_dfs[fact_name] = fact_table
    else:
        normalized_dfs["fact_table"] = fact_table

    return normalized_dfs


def main():
    # read source into dataframe
    # https://www.kaggle.com/datasets/rahulvyasm/netflix-movies-and-tv-shows
    df = pd.read_csv(
        "data/netflix_titles.csv",
        encoding="latin1",
    )

    """
    Column descripttions taken from kaggle
    Columns: There are 12 columns in the dataset:
    show_id: A unique identifier for each title.
    type: The category of the title, which is either 'Movie' or 'TV Show'.
    title: The name of the movie or TV show.
    director: The director(s) of the movie or TV show. (Contains null values for some entries, especially TV shows where this information might not be applicable.)
    cast: The list of main actors/actresses in the title. (Some entries might not have this information.)
    country: The country or countries where the movie or TV show was produced.
    date_added: The date the title was added to Netflix.
    release_year: The year the movie or TV show was originally released.
    rating: The age rating of the title.
    duration: The duration of the title, in minutes for movies and seasons for TV shows.
    listed_in: The genres the title falls under.
    description: A brief summary of the title.
    """

    # dropping columns with all null values
    df.dropna(axis=1, how="all", inplace=True)

    df = df.astype(object).where(pd.notnull(df), None)

    df[
        [
            "show_id",
            "type",
            "title",
            "director",
            "cast",
            "country",
            "rating",
            "duration",
            "listed_in",
            "description",
        ]
    ] = df[
        [
            "show_id",
            "type",
            "title",
            "director",
            "cast",
            "country",
            "rating",
            "duration",
            "listed_in",
            "description",
        ]
    ].astype(
        str
    )

    # removing whitespace from dates then then formatting
    df["date_added"] = df["date_added"].str.lstrip(" ")
    df["date_added"] = pd.to_datetime(df["date_added"])

    df["release_year"] = df["release_year"].astype("int16")

    df["listed_in"] = df["listed_in"].apply(lambda x: x.split(", "))
    df["country"] = df["country"].apply(lambda x: x.split(", "))
    df["cast"] = df["cast"].apply(lambda x: x.split(", "))

    normalized_dfs = normalize(
        df,
        primary_key="show_id",
        fact_name="netflix",
        exclude=["description", "title"],
        exploders=["listed_in", "country", "cast"],
    )


if __name__ == "__main__":
    try:
        main()
        logger.success("Script Finished")
    except Exception as e:
        logger.exception(e)
