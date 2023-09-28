#!/usr/bin/python

import subprocess
import sys
import os

target_dir = sys.argv[1]
version = sys.argv[2]
collections = ["customers", "orders", "stock_items", "products", "warehouses"]
queries = [
    "warehouse_revenue",
    "product_availability",
    "find_one_customer",
    "find_warehouses_for_order"
]


def path(base_path, name):
    return os.path.join(base_path, f"{name}.png")


for query_name in queries:
    subprocess.call(["python",
                     "histogram.py",
                     "http://localhost:9090",
                     f"transaction_duration_milliseconds_bucket{{operation=\"{query_name}\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
                     path(target_dir, f"transaction_{query_name}.png")])
    subprocess.call(["python",
                     "histogram.py",
                     "http://localhost:9090",
                     f"database_duration_milliseconds_bucket{{operation=\"{query_name}\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
                     path(target_dir, f"database_{query_name}.png")])

for collection in collections:
    subprocess.call([
        "python",
        "histogram.py",
        "http://localhost:9090",
        f"transaction_duration_milliseconds_bucket{{collection=\"{collection}\",operation=\"WRITE\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
        path(target_dir, f"transaction_{collection}_write.png")])
    subprocess.call([
        "python",
        "histogram.py",
        "http://localhost:9090",
        f"database_duration_milliseconds_bucket{{collection=\"{collection}\",operation=\"WRITE\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
        path(target_dir, f"database_{collection}_write.png")])
    subprocess.call([
        "python",
        "histogram.py",
        "http://localhost:9090",
        f"database_duration_milliseconds_bucket{{collection=\"{collection}\",operation=\"READ\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
        path(target_dir, f"database_{collection}_read.png")])

subprocess.call([
    "python",
    "histogram.py",
    "http://localhost:9090",
    f"database_duration_milliseconds_bucket{{operation!=\"warehouse_revenue\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
    path(target_dir, f"database_oltp.png")])

subprocess.call([
    "python",
    "histogram.py",
    "http://localhost:9090",
    f"transaction_duration_milliseconds_bucket{{operation!=\"warehouse_revenue\",phase=\"TRANSACTION\",otel_scope_version=~\"{version}\"}}",
    path(target_dir, f"transaction_oltp.png")])

subprocess.call([
    "python",
    "histogram.py",
    "http://localhost:9090",
    f"database_duration_milliseconds_bucket{{phase=\"LOAD\",otel_scope_version=~\"{version}\"}}",
    path(target_dir, f"database_load.png")])

subprocess.call([
    "python",
    "histogram.py",
    "http://localhost:9090",
    f"transaction_duration_milliseconds_bucket{{phase=\"LOAD\",otel_scope_version=~\"{version}\"}}",
    path(target_dir, f"transaction_load.png")])

