import pystac
import duckdb
import requests
import concurrent.futures
import os
import zipfile

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


class CollectionLogger(AppLogger):
    """
    Creates a collection log for collections already downloaded.
    Includes collection_id, collection_path, a success/completion boolean, and timestamp of log.
    """

    def __init__(self):
        pass

    def create_col_log(self):
        pass

    def log_col_downloaded(self):
        pass

    def log_col_unzipped(self):
        pass

    def log_col_translated(self):
        pass

    def check_col_downloaded(self):
        pass

    def check_col_unzipped(self):
        pass

    def check_col_translated(self):
        pass


class ResourceLogger(AppLogger):
    """
    Creates a resource log for resource zips downloaded.
    Includes collection_id, resource_id, unzip_path, a success/completion boolean, and timestamp of log.
    """

    def __init__(self):
        pass

    def create_rsc_log(self):
        pass

    def log_rsc_downloaded(self):
        pass

    def log_rsc_unzipped(self):
        pass

    def log_rsc_translated(self):
        pass

    def check_rsc_downloaded(self):
        pass

    def check_rsc_unzipped(self):
        pass

    def check_rsc_translated(self):
        pass


class AssetLogger(AppLogger):
    """
    Creates an asset log for each asset created from unzipped resources.
    includes their local unzip path, local cng path, success/completion boolean, and timestamp of log.
    """

    def __init__(self):
        pass

    def create_asset_log(self):
        pass

    def log_asset_


class UploadLogger(AppLogger):
    """
    Creates an upload log for each asset uploaded to cloud storage.
    includes their local cng path, their upload url, success/completion bool, and timestamp of log.
    """

    def __init__(self):
        pass


class STACLogger(AppLogger):
    """
    Creates a log for each upload cataloged in the STAC catalog json file. 
    """

    def __init__(self):
        pass


class Resource:
    def __init__(self, logger: AppLogger):
        pass

    def check_rsc_unzipped(self):
        pass

    def check_rsc_translated(self):
        pass

    def log_rsc_unzipped(self):
        pass

    def log_rsc_translated(self):
        pass


class Collection:
    def __init__(self, logger: AppLogger):
        pass

    def get_resources(self):
        pass


class App():
    def __init__(self):
        # init app with all i/o path settings
        # create directories if not exist
        # init logger class and
        # logger class inits log files if not exists
        TXGIO_ASSET_ZIPPED_BASEPATH = f"unprocessed_data/zip"
        TXGIO_ASSET_UNZIP_BASEPATH = f"unprocessed_data/unzip"
        # write the resource id overlaps into the stac catalog if path.is_exist = True
        TXGIO_ASSET_CNG_BASEPATH = f"processed_data/assets"

        pass

    def get_collections():
        pass

    def duckdb_connect():
        pass
