from scrape import seasons, comps, cats
import pandas as pd
import os
import numpy as np
import re


def clean_data():
    """Removes duplicate data and keeps only data which is a total (i.e. not percentages, averages, divisions)."""
    for cat, totals in cats.items():
        for comp in comps:
            for season in seasons:
                df = pd.read_csv(f"raw/{comp}/{cat}/{season}.csv", index_col=0).fillna(0)[totals]
                directory = f"data/{comp}/{cat}"
                if not os.path.exists(directory):
                    os.makedirs(directory)
                df.to_csv(f"{directory}/{season}.csv")


def avg_to_total():
    """Converts three advanced goalkeeper stats from averages to totals"""
    for comp in comps:
        for season in seasons:
            df_clean = pd.read_csv(f"data/{comp}/keepersadv/{season}.csv", index_col=0)
            df_raw = pd.read_csv(f"raw/{comp}/keepersadv/{season}.csv", index_col=0).fillna(0)
            df_clean["TotLen"] = (df_raw["Att.1"] * df_raw["AvgLen"]).round(2)
            df_clean["TotLen.1"] = (df_raw["Att.2"] * df_raw["AvgLen.1"]).round(2)
            df_clean["TotDist"] = (df_raw["#OPA"] * df_raw["AvgDist"]).round(2)
            df_clean.to_csv(f"data/{comp}/keepersadv/{season}.csv")


def gk_mins():
    """Merges duplicate players if they have played for two teams. Adds goalkeeper minutes to playingtime."""
    for comp in comps:
        for season in seasons:
            df_raw = pd.read_csv(f"raw/{comp}/keepers/{season}.csv")
            df_raw = df_raw.groupby("id").sum(numeric_only=True).reset_index()
            df_raw = df_raw.set_index("id")
            df_clean = pd.read_csv(f"data/{comp}/playingtime/{season}.csv")
            df_clean = df_clean.groupby("id").sum(numeric_only=True).reset_index()
            df_clean = df_clean.set_index("id")
            for key in ["Min", "MP", "Starts", "90s"]:
                df_clean[f"{key}GK"] = df_raw[key]
            df_clean = df_clean.fillna(0)
            for key in ["90s", "90sGK", "PPM", "On-Off", "On-Off.1"]:
                df_clean[key] = df_clean[key].round(2)
            for key in ["Min", "onG", "MinGK", "MPGK", "StartsGK"]:
                df_clean[key] = df_clean[key].astype(int)
            df_clean.to_csv(f"data/{comp}/playingtime/{season}.csv")


def player_info():
    """Cleans and concatenates player info into a single csv"""
    types = {"id": "string", "Player": "string", "Nation": "string", "Min": "int32"}
    aggregate = {"Player": "last", "Nation": "last", "Season": lambda x: '/'.join(x.unique())}
    dfs = []
    for comp in comps:
        for season in seasons:
            df = pd.read_csv(f"raw/{comp}/stats/{season}.csv").fillna(0)[types.keys()]
            df = df.astype(types)
            df["Season"] = "2022-2023" if season == "" else season
            df = df[df["Min"] > 0]
            dfs.append(df)
    df = pd.concat(dfs, ignore_index=True).groupby(["id"], as_index=True).agg(aggregate)
    nations = pd.read_csv("data/nation_abbreviations.csv", index_col="abbr")
    df["Nation"] = np.vectorize(lambda x: nations.loc[(x.split()[-1]).strip(), "name"])(df["Nation"])
    df["Season"] = np.vectorize(lambda x: int([y for y in x.split("/") if y != ""][-1][-2:]))(df["Season"])
    df = df.drop(columns=["Season"])
    df.to_csv("data/player_info.csv")


def convert_value(value):
    """Converts the string value string from transfermarkt into a decimal (representing millions of euros)"""
    if value == "-":
        return 0
    elif value[-1] == "k":
        return round(float(re.sub("[^0123456789\.]", "", value)) / 1000, 1)
    else:
        return round(float(re.sub("[^0123456789\.]", "", value)), 1)


def main():
    # clean_data()
    # avg_to_total()
    # gk_mins()
    player_info()


if __name__ == "__main__":
    main()
