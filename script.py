import csv
import duckdb
import os
import pandas as pd
from string import Template
from typing import List


COLLECTIONS = ['447db89a-58ee-4a1b-a61f-b918af2fb0bb',
               '6d9c4a2e-b5bb-49b3-9ceb-0727f4711c5b', '3e1530dc-d5be-4683-811b-9d4b162f0aa9']
TXGIO_ASSET_ZIPPED_BASEPATH = "unprocessed_data/zip"
TXGIO_ASSET_UNZIP_BASEPATH = "unprocessed_data/unzip"
# write the resource id overlaps into the stac catalog if path.is_exist = True
TXGIO_ASSET_CNG_BASEPATH = "processed_data/catalog"
TXGIO_ASSET_ZIPPED_PATH = Template(
    "${TXGIO_ASSET_ZIPPED_BASEPATH}/${collection_id}/${resource_id}")
TXGIO_ASSET_UNZIP_PATH = Template(
    "${TXGIO_ASSET_UNZIP_BASEPATH}/${collection_id}/${resource_id}")
TXGIO_ASSET_CNG_PATH = Template(
    "${TXGIO_ASSET_CNG_BASEPATH}/${collection_id}/${resource_id}")

TXGIO_LOG_BASEPATH = "logs"
TXGIO_RESOURCE_DL_LOG = f"{TXGIO_LOG_BASEPATH}/resources_downloaded.csv"
TXGIO_RESOURCE_UNZIP_LOG = f"{TXGIO_LOG_BASEPATH}/resources_unzipped.csv"
TXGIO_RESOURCE_ASSET_TRANSLATE_LOG = f"{TXGIO_LOG_BASEPATH}/assets_translated.csv"
TXGIO_CNG_ASSET_UPLOADED_LOG = f"{TXGIO_LOG_BASEPATH}/cng_assets_uploaded.csv"


def create_log_if_not_exists(file_path: str, headers: List[str]):
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            f.write(', '.join(headers))


def get_collections() -> pd.DataFrame:
    con = duckdb.connect(database=":memory:", read_only=False)
    coll_str = ""
    for x in COLLECTIONS:
        coll_str += f"'{x}',"
    sql = f"select * from '../data_txgio/collections.parquet' where collection_id in ({coll_str})"
    print(f"GETTING COLLECTIONS: {sql}")

    res = con.execute(sql)
    data = res.df()

    con.close()

    return data


def get_resources_by_collection_id(collection_id) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:", read_only=False)

    sql = f"select * from '../data_txgio/resources.parquet' where collection_id='{collection_id}'"
    print(f"GETTING RESOURCES FOR COLLECTION {collection_id}: {sql}")

    res = con.execute(sql)
    data = res.df()

    con.close()

    return data


def get_resource_dl_log():
    create_log_if_not_exists(TXGIO_RESOURCE_DL_LOG, [
                             "resource_id", "resource_path"])
    log = pd.read_csv(TXGIO_RESOURCE_DL_LOG)

    return log


def is_resource_downloaded():
    """checks if resource is in the download log by either resource_id or filename"""
    # make duckdb connection
    con = duckdb.connect(database=":memory:", read_only=True)
    sql = f"select * from '{TXGIO_RESOURCE_DL_LOG}' where resource_id = {resource_id} or filename = {filename}"
    res = con.execute(sql)
    data = res.df()

    return data


def download_resource(resource):
    # check if resource already in dl log

    pass


rsc_dl_log = get_resource_dl_log()
print(rsc_dl_log)

collections = get_collections()
print(collections)
for idx, c in collections.iterrows():
    print(c["name"])

resources = get_resources_by_collection_id(collections.at[0, "collection_id"])
print(resources)
