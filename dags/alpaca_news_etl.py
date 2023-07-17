import requests
import json
import pandas as pd
import config

from datetime import datetime, timedelta


def extract_alpaca_news(apiKey, apiSecret, symbols, startDate, endDate):
    """
    Extracts news data from Alpaca API and returns a dict.

    :param apiKey:
    :param apiSecret:
    :param symbols: ex.:AMD
    :param startDate: ex.: 2021-01-01T00:00:00Z
    :param endDate:
    :return:
    """

    url = f'https://data.alpaca.markets/v1beta1/news?start={startDate}&end={endDate}&symbols={symbols}&limit=50'
    headers = {'content-type': 'application/json', 'Apca-Api-Key-Id': apiKey, 'Apca-Api-Secret-Key': apiSecret}
    response = requests.get(url, headers=headers, verify=False)
    return json.loads(response.text)


def transform_alpaca_news_api_response(responseDict):
    """
    Transforms the response from the Alpaca API to a pandas dataframe.

    :param responseDict:
    :return:
    """
    listWithNewsData = []
    for i in range(len(responseDict["news"])):
        try:
            headline = responseDict["news"][i]["headline"]
            symbols = ", ".join(responseDict["news"][i]["symbols"])
            timeStamp = responseDict["news"][i]["updated_at"]
            newsId = responseDict["news"][i]["id"]
            content = responseDict["news"][i]["content"]
            listWithNewsData.append([headline, symbols, timeStamp, newsId, content])
        except:
            pass

    return pd.DataFrame(listWithNewsData, columns=["headline", "symbols", "time_stamp", "newsId", "content"])

def rrtest():
    print(111)

if __name__ == "__main__":
    now = datetime.now()
    df = transform_alpaca_news_api_response(extract_alpaca_news(config.ALPACA_API_KEY, config.ALPACA_SECRET_KEY, "AMD,TSLA,BTC",
                                                                startDate="2021-01-01", endDate=now.strftime("%Y-%m-%dT%H:%M:%SZ")))
    print(df)