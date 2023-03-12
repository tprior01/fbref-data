from bs4 import BeautifulSoup, Comment
import requests
import pandas as pd
import time
import numpy as np
from csv import writer
from sortedcontainers import SortedSet
import random
from dotenv import load_dotenv
from serpapi import GoogleSearch
import os

load_dotenv()

GoogleSearch.SERP_API_KEY = os.environ["serapi_key"]

seasons = [
    "2017-2018",
    "2018-2019",
    "2019-2020",
    "2020-2021",
    "2021-2022",
    "2022-2023"
]

# fbref competitions and their corresponding id number
comps = {
    "Premier-League": 9,
    "Serie-A": 11,
    "La-Liga": 12,
    "Ligue-1": 13,
    "Bundesliga": 20,
    "Champions-League": 8,
    "Europa-League": 19
}

# fbref categories and a list of that tables items which are 'totals' i.e. not derived from other properties
cats = {
    "stats",
    "keepers",
    "keepersadv",
    "shooting",
    "passing",
    "passing_types",
    "gca",
    "defense",
    "possession",
    "playingtime",
    "misc"
}


def scrape_fbref_player_table(url, output):
    """Scrapes an individual player table for a valid fbref url and saves it as csv."""
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for i, each in enumerate(comments):
        if 'table' in str(each):
            try:
                players = BeautifulSoup(each, "html.parser").find_all("tr")
                player_ids = [x.find("td")["data-append-csv"] for x in players if x.find("td")]
                df = pd.read_html(str(each), header=1)[0]
                df = df[df['Rk'].ne('Rk')].reset_index(drop=True)
                df.set_index('Rk', inplace=True)
                df["id"] = player_ids
            except:
                continue
            df = df.set_index("id")
            df.to_csv(output)
            time.sleep(3)


def scrape_fbref_player_tables(cats, comps, seasons):
    """Iterates through fbref categories, competitions and seasons, saving each corresponding player table as a csv."""
    for cat in cats:
        for comp in comps:
            for season in seasons:
                if season == "2022-23":
                    url = f"https://fbref.com/en/comps/{comps[comp]}/{cat}/{comp}-stats"
                else:
                    url = f"https://fbref.com/en/comps/{comps[comp]}/{season}/{cat}/{season}-{comp}-stats"

                scrape_fbref_player_table(url, f"raw/{comp}/{cat}/{season}.csv")


def scrape_transfermarkt_page(url, headers):
    """Scrapes a transfermarkt page and returns a players club, position, current value and max value."""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    club = soup.find('span', itemprop="affiliation").text.strip()
    main_position = soup.find("dt", class_="detail-position__title", string="Main position:").findNextSibling().text.strip()
    try:
        other_position = soup.find("dt", class_="detail-position__title", string="Other position:").findNextSibling().text.strip()
    except:
        other_position = ""
    current_value = soup.find("div", class_="tm-player-market-value-development__current-value").text.strip()
    max_value = soup.find("div", class_="tm-player-market-value-development__max-value").text.strip()
    dob = soup.find("span", class_="info-table__content info-table__content--regular", string="Date of birth:").findNextSibling().text.strip()
    return club, main_position, other_position, current_value, max_value, dob


def get_map():
    """Returns a dataframe containing a map between fbref and transfermarkt urls and ids"""
    url = "https://raw.githubusercontent.com/JaseZiv/worldfootballR_data/master/raw-data/fbref-tm-player-mapping/output/fbref_to_tm_mapping.csv"
    df = pd.read_csv(url, encoding='latin-1')
    df["fbref_id"] = np.vectorize(lambda x: x.split("/")[-2])(df["UrlFBref"])
    df["tmrkt_id"] = np.vectorize(lambda x: x.split("/")[-1])(df["UrlTmarkt"])
    df = df.set_index("fbref_id")
    df = df[~df.index.duplicated(keep="last")]
    return df


def scrape_transfermarkt_pages():
    """
    Scrapes player information from transfermarkt for each player in player_info.csv and appends to a csv file. We also
    add transfermarkt urls to a csv file if they don't occur in the dataframe returned from get_map().
    """
    user_agent_list = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    ]
    # dataframe containing fbref ids mapped to transfermarket urls (provided by some kind soul online)
    df_tm = get_map()

    # dataframe containing player information from fbref (we have parsed this from scraped data)
    df_fb = pd.read_csv("data/player_info.csv", index_col=0)

    # dataframe of additional transfermarkt urls not in df_tm (we also remove any new overlap between df_tm and df_ad)
    df_ad = pd.read_csv("data/transfermarkt_urls.csv", index_col=0)
    df_ad = df_ad[~df_ad.index.isin(df_tm.index)]
    df_ad.to_csv("data/transfermarkt_urls.csv")

    # dataframe to which we will append transfermarkt information (we also remove any duplicates)
    df = pd.read_csv("data/transfermarkt_data.csv", index_col=0)
    df = df[~df.index.duplicated(keep="last")]
    df.to_csv("data/transfermarkt_data.csv")

    df_tm_ids = SortedSet(df_tm.index.to_list())
    df_ad_ids = SortedSet(df_ad.index.to_list())

    for id in set(df_fb.index.to_list()).difference(df.index.to_list()):
        try:
            if id in df_tm_ids:
                url = df_tm.loc[id].UrlTmarkt
            elif id in df_ad_ids:
                url = df_ad.loc[id].url
            else:
                print("trying...")
                # params = {
                #     "engine": "google",
                #     "q": f"transfermarkt {df_fb.loc[id].Player} {df_fb.loc[id].Nation} {df_fb.loc[id].Born}",
                #     "hl": "en",
                #     "gl": "uk",
                #     "num": 1,
                # }
                # search = GoogleSearch(params)
                # results = search.get_dict()
                # url = results['organic_results'][0]['link']
                # write_line('transfermarkt_urls.csv', [id, url])
                # time.sleep(6)
                continue
            write_line('data/transfermarkt_data.csv', [id, *scrape_transfermarkt_page(url, random.choice(user_agent_list))])
        except Exception as e:
            print(e)
            continue


def write_line(csv, line):
    """Appends the provided list as a line to the csv provided."""
    with open(csv, 'a') as f_object:
        writer_object = writer(f_object)
        print(line)
        writer_object.writerow(line)
        f_object.close()


def main():
    # scrape_fbref_player_tables(cats, comps, seasons)
    scrape_transfermarkt_pages()


if __name__ == "__main__":
    main()