import pystac
import duckdb
import requests
import concurrent.futures
import os
import zipfile
import csv
from string import Template
from typing import Callable

ddb_con = duckdb.connect(database=':memory:', read_only=False)

target_collections = ['447db89a-58ee-4a1b-a61f-b918af2fb0bb',
                      '6d9c4a2e-b5bb-49b3-9ceb-0727f4711c5b', '3e1530dc-d5be-4683-811b-9d4b162f0aa9']

sql = f"select * from '../data_txgio/collections.parquet' where collection_id='{target_collections[0]}'"

query = ddb_con.execute(sql)

collections = query.df().to_dict(orient="records")

ddb_con.close()

os.makedirs("collections", exist_ok=True)


class AppLogger:
    def __init__(self):
        pass


class CatalogLogger(AppLogger):
    """
    Creates a catalog log for STAC catalog created from subparts.
    Includes local_catalog_path, catalog_id, remote_catalog_path, a success/completion boolean, and timestamp of the log.
    """

    def __init__(self):
        pass


class CollectionLogger(CatalogLogger):
    """
    Creates a collection log for collections already downloaded.
    Includes collection_id, collection_path, a success/completion boolean, and timestamp of log.
    """

    def __init__(self):
        pass

    def create_col_log(self):
        """
        creates the collection log .csv if it does not already exist
        """
        pass

    def log_col_downloaded(self):
        """
        logs when the collection was downloaded in completion.
        in other words, when all resources in the collection have been downloaded and logged.
        """
        pass

    def log_col_unzipped(self):
        """
        logs when the collection has been completely unzipped.
        in other words, when all resources in the collection have been downloaded and logged,
        and when all downloads have been successfully unzipped and logged.
        """
        pass

    def log_col_translated(self):
        """
        logs when the collection has been completely downloaded,
        unzipped, and translated to cng format.
        all of these determinations should be made from
        previous logs in the rsc -> asset -> uploadasset logs
        """
        pass


class ResourceLogger(CollectionLogger):
    """
    Creates a resource log for resource zips downloaded.
    Includes collection_id, resource_id, unzip_path, a success/completion boolean, and timestamp of log.
    """

    def __init__(self):
        pass

    def create_rsc_log(self):
        """
        creates the resource_log.csv if it does not already exist.
        """
        pass

    def log_rsc_downloaded(self):
        """
        logs to the resource_log.csv when a resource is downloaded.
        """
        pass

    def log_rsc_unzipped(self):
        """
        logs to the resource_log.csv when a resource is unzipped.
        """
        pass


class AssetLogger(ResourceLogger):
    """
    Creates an asset log for each asset created from unzipped resources.
    includes their local unzip path, local cng path, success/completion boolean, and timestamp of log.
    """

    def __init__(self):
        pass

    def create_asset_log(self):
        """
        creates asset_log.csv if it does not already exist
        """
        pass

    def log_asset_unzipped(self):
        """
        logs each asset in a resource, and the path unzipped to.
        ??? Fields: resource_id, asset_name, and asset_unzip_path ???
        """


class AssetUploadLogger(AssetLogger):
    """
    Creates an upload log for each asset uploaded to cloud storage.
    includes their local cng path, their upload url, success/completion bool, and timestamp of log.
    """

    def __init__(self):
        pass


class Collection:
    """
    represents a collection in the txgio data catalog
    """

    def __init__(self, data: dict, logger: CollectionLogger):
        if isinstance(data, dict):
            for key, value in data.items():
                setattr(self, key, value)
        else:
            raise TypeError("Input data must be a dictionary")

    def get_resources(self):
        print(f"getting resources for collection {self.collection_id}")
        sql = f"select * from '../data_txgio/resources.parquet' where collection_id = '{self.collection_id}'"
        con = duckdb.connect(database=":memory:", read_only=False)
        res = con.execute(sql)
        data = res.df().to_dict(orient="records")

        con.close()

        return data

    def __str__(self):
        return vars(self).__str__()


class Resource:
    """
    represents a resource zip file downloaded from
    txgio data api
    """

    def __init__(self, collection: Collection, logger: ResourceLogger):
        pass

    def check_downloaded(self):
        # check which resources have already been downloaded
        pass

    def download_resource(self):
        # download resource
        pass


class Asset:
    """
    represents an asset unzipped from a resource
    zip file
    """

    def __init__(self, resource: Resource, logger: AssetLogger):
        pass


class AssetUpload:
    """
    represents a file uploaded to cloud object storage
    """

    def __init__(self, asset: Asset, logger: AssetUploadLogger):
        pass


class App():
    def __init__(self):
        # init app with all i/o path settings
        # create directories if not exist
        # init logger class and
        # logger class inits log files if not exists
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

        self.collections = []
        self.get_collections()

    def get_collections(self):
        con = self.duckdb_connect()
        sql = "select * from '../data_txgio/collections.parquet';"
        res = con.execute(sql)
        data = res.df().to_dict(orient="records")

        for c in data:
            col = Collection(c, CollectionLogger())
            print(col)
            self.collections.append(col)

        con.close()

    def get_collection_by_attribute(self, key: str, value: str, filter_fn: Callable[[str, str], bool]):
        results = []

        for c in self.collections:
            if c[key] and filter_fn(key, value):
                results.append(c)

        return results

    def duckdb_connect(self):
        con = duckdb.connect(database=":memory:", read_only=False)
        sql_install = f"INSTALL sqlite; LOAD sqlite; INSTALL spatial; LOAD spatial;"

        return con


def main():
    app = App()

    for c in app.collections:
        print(c)


if __name__ == "__main__":
    main()
