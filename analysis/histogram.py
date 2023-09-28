#!/usr/bin/python3
import os.path
import sys
import json
from urllib.request import urlopen
import pandas as pd
import matplotlib.pyplot as plt
from urllib import parse
from datetime import datetime
import math

# HISTOGRAM_NAME = "prometheus_http_response_size_bytes_bucket"
HISTOGRAM_NAME = "database_duration_milliseconds_bucket"

prometheus_url = sys.argv[1]
prometheus_query = sys.argv[2]
image_path = sys.argv[3]


def parse_le(value):
    return float(value)


def parse_result(bucket):
    return float(bucket["metric"]["le"]), float(bucket["value"][1])


def epoch_seconds():
    e = datetime(1970, 1, 1, 0, 0)
    now = datetime.now()
    return (now - e).total_seconds()


def make_request():
    query = parse.quote_plus(f"last_over_time({prometheus_query}[1y])")
    local_time = epoch_seconds()
    url = f"{prometheus_url}/api/v1/query?query={query}&time={local_time:.3f}"
    print(url)
    # url = f"{prometheus_url}/api/v1/query?query={HISTOGRAM_NAME}"
    with urlopen(url) as histogram_response:
        if histogram_response.getcode() < 200 or histogram_response.getcode() > 299:
            raise Exception("failed to fetch histogram from prometheus")
        histogram_json = json.loads(histogram_response.read())
        results = list()
        for result in histogram_json["data"]["result"]:
            results.append(parse_result(result))

        return pd.DataFrame(results, columns=["le", "count_cum"])


def convert_inf(value):
    if value == float("inf"):
        # one day
        return 86400000
    else:
        return value


def clean_data(raw_data):
    raw_data["le"] = raw_data["le"].map(convert_inf)
    # raw_data = raw_data[raw_data["le"] < 1205000.0]
    raw_data = raw_data.groupby(by=["le"]).sum().sort_values("le").reset_index()
    try:
        raw_data["count_cum"][1::] = raw_data["count_cum"].diff()[1::]
    except ValueError:
        pass
    try:
        max_le = raw_data[raw_data["count_cum"] != 0].iloc[-1]["le"]
        raw_data = raw_data[raw_data["le"] <= max_le]
    except IndexError as e:
        pass
    return raw_data


# returns the bins for the data
# The steps in the le scale are not linear, but we want the buckets to be equal size.
# Therefore, the best possible solution is chosen by using the difference between the largest two le values.
def bins(all_data):
    last_values = list(all_data["le"][-2::])
    if len(last_values) == 0:
        return []
    elif len(last_values) == 1:
        return [0, last_values[0]]
    else:
        return sorted([x + 0.0001 for x in range(math.ceil(last_values[1]), -1, int(last_values[0] - last_values[1]))])


def describe(weighted_data):
    durations = []
    for row in weighted_data.itertuples():
        durations_list = []
        for i in range(int(row.count_cum)):
            durations_list.append(row.le)
        durations.append(pd.DataFrame(durations_list, columns=["duration"]))
    try:
        durations = pd.concat(durations, ignore_index=True)
    except ValueError:
        durations = pd.DataFrame()
    with open(os.path.splitext(image_path)[0] + ".txt", "w") as file:
        if durations.size != 0:
            file.write(str(durations.describe(percentiles=[.25, .5, .75, 0.9, 0.99, 0.999])))
            file.write("\n")
        else:
            file.write("empty dataset\n")

if __name__ == "__main__":
    data = make_request()
    data = clean_data(data)
    if data.size == 0:
        print(f"DataFrame for {prometheus_query} is empty")
    describe(data.copy(deep=True))
    fig, ax = plt.subplots()
    ax.hist(data["le"], bins=bins(data), weights=data["count_cum"], log=True, edgecolor="white")
    ax.set_xlabel("Dauer in ms")
    ax.set_ylabel("Absolute Häufigkeit")
    plt.savefig(image_path)
