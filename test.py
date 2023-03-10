from bs4 import BeautifulSoup, Comment
import requests
import pandas as pd
import time
import numpy as np
from csv import writer
from googlesearch import search
from sortedcontainers import SortedSet

url = "https://fbref.com/en/matches/e62f6e78/Crystal-Palace-Arsenal-August-5-2022-Premier-League"
response = requests.get(url)
print(pd.read_html(str(response), header=1))
soup = BeautifulSoup(response.text, "html.parser")
# print(soup)
#
# comments = soup.find_all(string=lambda text: isinstance(text, Comment))
# for each in comments:
#     if "table" in str(each):
#         try:
#             players = BeautifulSoup(each, "html.parser").find_all("tr")
#             player_ids = [x.find("td")["raw-append-csv"] for x in players if x.find("td")]
#             df = pd.read_html(str(each), header=1)[0]
#             df = df[df["Rk"].ne("Rk")].reset_index(drop=True)
#             df.set_index("Rk", inplace=True)
#             df["id"] = player_ids
#             df = df.set_index("id")
#         except:
#             continue
#         df.to_csv(output)
#         time.sleep(3)