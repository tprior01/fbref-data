from bs4 import BeautifulSoup, Comment
import requests
import pandas as pd
import time

comps = {
    "Premier-League": {"id": 9, "abbr": "ENG"},
    "Serie-A": {"id": 11, "abbr": "ITA"},
    "La-Liga": {"id": 12, "abbr": "SPA"},
    "Ligue-1": {"id": 13, "abbr": "FRA"},
    "Bundesliga": {"id": 20, "abbr": "GER"},
    "Champions-League": {"id": 8, "abbr": "CL"},
    "Europa-League": {"id": 19, "abbr": "EL"}
}

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
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for i, each in enumerate(comments):
        if 'table' in str(each):
            try:
                players = BeautifulSoup(each, "html.parser").find_all("tr")
                player_ids = [x.find("td")["data2-append-csv"] for x in players if x.find("td")]
                df = pd.read_html(str(each), header=1)[0]
                df = df[df['Rk'].ne('Rk')].reset_index(drop=True)
                df.set_index('Rk', inplace=True)
                df["id"] = player_ids
            except:
                continue
            df = df.set_index("id")
            df.to_csv(output)
            time.sleep(3)


def scrape_fbref_season(season):
    for cat in cats:
        for name, info in comps.items():
            url = f"https://fbref.com/en/comps/{info['no']}/{cat}/{name}-stats"
            scrape_fbref_player_table(url, f"data/{info['abbr']}/{cat}/{season}.csv")


def main():
    scrape_fbref_season(23)


if __name__ == "__main__":
    main()
