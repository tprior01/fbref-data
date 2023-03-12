from scrape import seasons, comps
import pandas as pd
import os
import numpy as np
import re

cats = {
    "keepers": {'GA': 'int', 'SoTA': 'int', 'Saves': 'int', 'W': 'int', 'D': 'int', 'L': 'int', 'CS': 'int', 'PKatt': 'int', 'PKA': 'int', 'PKsv': 'int', 'PKm': 'int'},
    "keepersadv": {'FK': 'int', 'CK': 'int', 'OG': 'int', 'PSxG': 'float', 'PSxG+/-': 'float', 'Cmp': 'int', 'Att': 'int', 'Att.1': 'int', 'Thr': 'int', 'Att.2': 'int', 'Opp': 'int', 'Stp': 'int', '#OPA': 'int'},
    "shooting": {'Gls': 'int', 'Sh': 'int', 'SoT': 'int', 'FK': 'int', 'PK': 'int', 'PKatt': 'int', 'xG': 'float', 'npxG': 'float', 'G-xG': 'float', 'np:G-xG': 'float'},
    "passing": {'Cmp': 'int', 'Att': 'int', 'TotDist': 'float', 'PrgDist': 'float', 'Cmp.1': 'int', 'Att.1': 'int', 'Cmp.2': 'int', 'Att.2': 'int', 'Cmp.3': 'int', 'Att.3': 'int', 'Ast': 'int', 'xAG': 'float', 'xA': 'float', 'A-xAG': 'float', 'KP': 'int', '1/3': 'int', 'PPA': 'int', 'CrsPA': 'int', 'PrgP': 'int'},
    "passing_types": {'Live': 'int', 'Dead': 'int', 'FK': 'int', 'TB': 'int', 'Sw': 'int', 'Crs': 'int', 'TI': 'int', 'CK': 'int', 'In': 'int', 'Out': 'int', 'Str': 'int', 'Off': 'int', 'Blocks': 'int'},
    "gca": {'SCA': 'int', 'PassLive': 'int', 'PassDead': 'int', 'TO': 'int', 'Sh': 'int', 'Fld': 'int', 'Def': 'int', 'GCA': 'int', 'PassLive.1': 'int', 'PassDead.1': 'int', 'TO.1': 'int', 'Sh.1': 'int', 'Fld.1': 'int', 'Def.1': 'int'},
    "defense": {'Tkl': 'int', 'TklW': 'int', 'Def 3rd': 'int', 'Mid 3rd': 'int', 'Att 3rd': 'int', 'Tkl.1': 'int', 'Att': 'int', 'Lost': 'int', 'Blocks': 'int', 'Sh': 'int', 'Pass': 'int', 'Int': 'int', 'Tkl+Int': 'int', 'Clr': 'int', 'Err': 'int'},
    "possession": {'Touches': 'int', 'Def Pen': 'int', 'Def 3rd': 'int', 'Mid 3rd': 'int', 'Att 3rd': 'int', 'Att Pen': 'int', 'Live': 'int', 'Att': 'int', 'Succ': 'int', 'Tkld': 'int', 'Carries': 'int', 'TotDist': 'float', 'PrgDist': 'float', 'PrgC': 'int', '1/3': 'int', 'CPA': 'int', 'Mis': 'int', 'Dis': 'int', 'Rec': 'int', 'PrgR': 'int'},
    "playingtime": {'MP': 'int', 'Min': 'int', '90s': 'int', 'Starts': 'int', 'Compl': 'int', 'Subs': 'int', 'unSub': 'int', 'onG': 'int', 'onGA': 'int', '+/-': 'int', 'onxG': 'float', 'onxGA': 'float', 'xG+/-': 'float'},
    "misc": {'CrdY': 'int', 'CrdR': 'int', '2CrdY': 'int', 'Fls': 'int', 'Fld': 'int', 'Off': 'int', 'PKwon': 'int', 'PKcon': 'int', 'OG': 'int'}
}


def open_and_group_by(csv):
    """Opens a csv and groups by id"""
    df = pd.read_csv(csv, index_col=0).fillna(0)
    df = df.groupby("id").sum(numeric_only=True).reset_index()
    df = df.set_index("id")
    return df


def clean_data():
    """Removes duplicate data and keeps only data which is a total (i.e. not percentages, averages, divisions)."""
    for cat, totals in cats.items():
        for comp in comps:
            for season in seasons:
                df = open_and_group_by(f"raw/{comp}/{cat}/{season}.csv")
                df = df[totals.keys()]
                df = df.astype(cats[cat])
                directory = f"data/{comp}/{cat}"
                if not os.path.exists(directory):
                    os.makedirs(directory)
                df.to_csv(f"{directory}/{season}.csv")


def avg_to_total():
    """Converts four stats from averages to totals"""
    for comp in comps:
        for season in seasons:
            # goalkeeper stats
            df_clean = pd.read_csv(f"data/{comp}/keepersadv/{season}.csv", index_col=0)
            df_raw = open_and_group_by(f"raw/{comp}/keepersadv/{season}.csv")
            df_clean["TotLen"] = (df_raw["Att.1"] * df_raw["AvgLen"]).round(2)
            df_clean["TotLen.1"] = (df_raw["Att.2"] * df_raw["AvgLen.1"]).round(2)
            df_clean["TotDist"] = (df_raw["#OPA"] * df_raw["AvgDist"]).round(2)
            df_clean.to_csv(f"data/{comp}/keepersadv/{season}.csv")
            # shooting stats
            df_clean = pd.read_csv(f"data/{comp}/shooting/{season}.csv", index_col=0)
            df_raw = open_and_group_by(f"raw/{comp}/shooting/{season}.csv")
            df_clean["TotDist"] = (df_raw["Sh"] * df_raw["Dist"]).round(2)
            df_clean.to_csv(f"data/{comp}/shooting/{season}.csv")


def gk_mins():
    """Adds goalkeeper minutes to playingtime."""
    for comp in comps:
        for season in seasons:
            df_raw = open_and_group_by(f"raw/{comp}/keepers/{season}.csv")
            df_clean = pd.read_csv(f"data/{comp}/playingtime/{season}.csv", index_col=0)
            for key in ["Min", "MP", "Starts", "90s"]:
                df_clean[f"{key}GK"] = df_raw[key]
            df_clean = df_clean.fillna(0)
            for key in ["90s", "90sGK"]:
                df_clean[key] = df_clean[key].round(2)
            for key in ["Min", "onG", "MinGK", "MPGK", "StartsGK", "90sGK"]:
                df_clean[key] = df_clean[key].astype(int)
            df_clean.to_csv(f"data/{comp}/playingtime/{season}.csv")


def recoveries_to_defense():
    """Adds recoveries to defense"""
    for comp in comps:
        for season in seasons:
            df_misc = open_and_group_by(f"raw/{comp}/misc/{season}.csv")
            df_def = pd.read_csv(f"data/{comp}/defense/{season}.csv", index_col=0)
            df_def["recov"] = df_misc["Recov"].astype(int)
            df_def.to_csv(f"data/{comp}/defense/{season}.csv")


def player_info():
    """Cleans and concatenates player info into a single csv"""
    types = {"id": "string", "Player": "string", "Nation": "string", "Min": "int32"}
    aggregate = {"Player": "last", "Nation": "last"}
    dfs = []
    for comp in comps:
        for season in seasons:
            df = pd.read_csv(f"raw/{comp}/stats/{season}.csv").fillna(0)[types.keys()]
            df = df.astype(types)
            df = df[df["Min"] > 0]
            dfs.append(df)
    df = pd.concat(dfs, ignore_index=True).groupby(["id"], as_index=True).agg(aggregate)
    nations = pd.read_csv("data/nation_abbreviations.csv", index_col="abbr")
    df["Nation"] = np.vectorize(lambda x: nations.loc[(x.split()[-1]).strip(), "name"])(df["Nation"])
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
    clean_data()
    avg_to_total()
    gk_mins()
    recoveries_to_defense()
    player_info()


if __name__ == "__main__":
    main()
