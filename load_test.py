import requests
import json
import os
import pandas as pd
import time
import random

api_url = "https://poc-assignment2.azurewebsites.net/"


def write_to_csv(data, filename):
    index = [i for i in range(len(data))]
    df = pd.DataFrame(data, index=index)
    df.to_csv(filename, index=True)
    print(f"Data written to {filename}")


def test_post():
    """
    Test the POST endpoint
    :return:
    curl -X 'POST' \
    'https://poc-assignment2.azurewebsites.net/query_top_n?n=10&from_year=2000&to_year=2022' \
    -H 'accept: application/json' \
    -d ''
    """
    data = []
    for i in range(100):
        n = random.randint(1, 10)
        from_year = random.randint(1995, 2005)
        to_year = random.randint(2015, 2023)

        url = api_url + f"query_top_n?n={n}&from_year={from_year}&to_year={to_year}"
        headers = {'accept': 'application/json'}
        print(f"Testing: {url}")
        a = time.time()
        response = requests.post(url, headers=headers, data=None)
        b = time.time()
        print(response.status_code)
        print(response.json())
        print(f"Time taken: {b - a}")
        data.append({"n": n, "from_year": from_year, "to_year": to_year, "time_taken": b - a})

    write_to_csv(data, "results.csv")



if __name__ == '__main__':
    test_post()
