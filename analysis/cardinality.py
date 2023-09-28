#!/usr/bin/python
import sys
from pymongo.mongo_client import MongoClient
from scipy.stats import binom, chisquare, geom
import pandas as pd
import matplotlib.pyplot as plt
import os

# generates diagrams for the cardinalities between two documents in the logistic benchmark

uri = sys.argv[1]
target_dir = sys.argv[2]
mode = sys.argv[3]

client = MongoClient(uri)
databases = client.list_databases()
db = None
for database_name in client.list_database_names():
    print(database_name)
    if database_name.isdigit():
        db = client.get_database(database_name)

if mode == "referencing":
    product_count_pipeline = [
        {"$group": {
            "_id": "$product_id",
            "count": {"$sum": 1}
        }}
    ]
elif mode == "embedding":
    product_count_pipeline = [
        {"$group": {
            "_id": "$product._id",
            "count": {"$sum": 1}
        }}
    ]
else:
    raise ValueError(f"{mode} must be either referencing or embedding")

N_PRODUCTS = 100_000

def product_goodness_of_fit(product_df, n_stock_items):
    print("product_goodness_of_fit")
    p = 1 / N_PRODUCTS
    lower_bound = -1
    for k in range(0, 2000):
        if binom.pmf(k, n_stock_items, p) * N_PRODUCTS > 5:
            lower_bound = k -1
            break
    if lower_bound == -1:
        raise AssertionError("could not find a lower bound satisfying the chi 5 samples per bin requirement")
    upper_bound = -1
    for k in range(lower_bound +1, 10000):
        if binom.pmf(k, n_stock_items, p) * N_PRODUCTS < 5:
            upper_bound = k
            break
    if upper_bound == -1:
        raise AssertionError("could not find an upper bound satisfying the chi 5 samples per bin requirement")

    print(f"lower_bound: {lower_bound}")
    print(f"upper_bound: {upper_bound}")

    ks = [k for k in range(lower_bound, upper_bound+1)]
    expected_frequencies = []
    lower_bound_sum = 0
    for k in range(0, lower_bound+1):
        lower_bound_sum += binom.pmf(k, n_stock_items, p) * N_PRODUCTS
    expected_frequencies.append(lower_bound_sum)
    for k in range(lower_bound+1, upper_bound):
        expected_frequencies.append(binom.pmf(k, n_stock_items, p) * N_PRODUCTS)
    upper_bound_sum = 0
    for k in range(upper_bound, N_PRODUCTS):
        upper_bound_sum += binom.pmf(k, n_stock_items, p) * N_PRODUCTS
    expected_frequencies.append(upper_bound_sum)

    expected_frequencies = pd.Series(expected_frequencies, index=ks).sort_index()
    print(f"expected_frequencies: {expected_frequencies}")

    product_df = product_df.groupby(by=["count"]).size()
    product_df.loc[lower_bound] = product_df.loc[product_df.index <= lower_bound].sum().squeeze()
    product_df.loc[upper_bound] = product_df.loc[product_df.index >= upper_bound].sum().squeeze()
    product_df = product_df.sort_index()
    product_df = product_df.truncate(before = lower_bound, after = upper_bound)

    table = pd.concat([product_df, expected_frequencies], axis=1).rename(columns={0: "observed", 1: "expected"})
    table = table.fillna(0)
    print(table)
    return chisquare(table["observed"], table["expected"])


stock_items_count = db["stock_items"].count_documents({})
product_occurrences = db["stock_items"].aggregate(product_count_pipeline)
product_df = pd.DataFrame(product_occurrences, columns=["_id", "count"])

fig, ax = plt.subplots()
ax.hist(product_df["count"], bins="sturges", edgecolor="white")
ax.set_xlabel("Anzahl an Referenzen auf ein Produkt")
ax.set_ylabel("Absolute Häufigkeit")
# ax.set_title("Häufigkeitsverteilung für Referenzen auf ein Produkt")
plt.savefig(os.path.join(target_dir, "product_occurrences.png"))


with open(os.path.join(target_dir, "product_occurrences_description.txt"), "w") as file:
    file.write(str(product_df.describe()))
    file.write("\n")
    count = db["stock_items"].count_documents({})
    file.write(f"db.stock_items.count(): {count}\n")
    fit = product_goodness_of_fit(product_df, db["stock_items"].count_documents({}))
    file.write(f"goodness of fit: {fit}\n")


def customer_goodness_of_fit(customer_df, n_customers):
    print("customer_goodness_of_fit")
    p = 1/10
    lower_bound = 1
    upper_bound = -1
    for k in range(lower_bound, 1000):
        if geom.pmf(k, p) * n_customers < 5:
            upper_bound = k
            break
    if upper_bound == -1:
        raise AssertionError("could not find an upper bound satisfying the chi 5 samples per bin requirement")

    print(f"upper_bound: {upper_bound}")

    expected_frequencies = []
    ks = [k for k in range(lower_bound, upper_bound+1)]
    for k in range(lower_bound, upper_bound):
        expected_frequencies.append(geom.pmf(k, p) * n_customers)
    upper_bound_sum = 0
    for k in range(upper_bound, 1000):
        upper_bound_sum += geom.pmf(k, p) * n_customers
    expected_frequencies.append(upper_bound_sum)
    expected_frequencies = pd.Series(expected_frequencies, index=ks).sort_index()
    print(f"expected_frequencies: {expected_frequencies}")

    customer_df = customer_df.groupby(by=["referenced_objects"]).size()
    customer_df.loc[upper_bound] = customer_df.loc[customer_df.index >= upper_bound].sum().squeeze()
    customer_df = customer_df.sort_index()
    customer_df = customer_df.truncate(after = upper_bound)

    table = pd.concat([customer_df, expected_frequencies], axis=1).rename(columns={0: "observed", 1: "expected"})
    table = table.fillna(0)
    print(table)
    return chisquare(table["observed"], table["expected"])

if mode == "referencing":
    customer_referenced_orders_pipeline = [
            {"$project": {"_id": 0, "referenced_objects": {"$size": "$order_ids"}}}
            ]
elif mode == "embedding":
    customer_referenced_orders_pipeline = [
        {"$project": {"_id": 0, "referenced_objects": {"$size": "$orders"}}}
    ]
else:
    raise ValueError(f"{mode} must be either referencing or embedding")

customer_referenced_orders = db["customers"].aggregate(customer_referenced_orders_pipeline)
customer_df = pd.DataFrame(customer_referenced_orders, columns=["referenced_objects"])

fix, ax = plt.subplots()
ax.hist(customer_df["referenced_objects"], bins="sturges", edgecolor="white")
ax.set_xlabel("Anzahl an referenzierten Orders pro Customer")
ax.set_ylabel("Absolute Häufigkeit")
# ax.set_title("Häufigkeitsverteilung für Anzahl an referenzierten Orders pro Customer")
plt.savefig(os.path.join(target_dir, "customer_referenced_orders.png"))

with open(os.path.join(target_dir, "customer_referenced_orders_description.txt"), "w") as file:
    file.write(str(customer_df.describe()))
    file.write("\n")
    count = db["customers"].count_documents({})
    file.write(f"db.customers.count(): {count}\n")
    fit = customer_goodness_of_fit(customer_df, count)
    file.write(f"goodness of fit: {fit}\n")
