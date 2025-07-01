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


class SqliteLogger:

    def __init__(self, dbpath="download_logs.db"):
        self.dbpath = dbpath
        self.log_table = "download_logs"

        con = self.get_connection()
        sql_create_table = f"CREATE TABLE IF NOT EXISTS {self.log_table} (resource VARCHAR, local_path VARCHAR, unzip_path VARCHAR, remote_path VARCHAR)"
        sql_validate_table = f"DESCRIBE SELECT * FROM {self.log_table}"
        create_res = con.execute(sql_create_table)
        print(create_res.fetchdf())
        validate_res = con.execute(sql_validate_table)
        print(validate_res.fetchdf())
        con.close()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(database=":memory:", read_only=False)
        sql_install = f"INSTALL sqlite; LOAD sqlite;"
        sql_attach = f"ATTACH '{self.dbpath}' AS sqlite_db (TYPE sqlite);"

        return con


class Collection:
    resources = []

    def __init__(self, data: dict, logger: SqliteLogger):
        self.logger = logger
        if isinstance(data, dict):
            for key, value in data.items():
                setattr(self, key, value)
        else:
            raise TypeError("Input data must be a dictionary")

        os.makedirs(f"collections/{self.collection_id}", exist_ok=True)
        os.makedirs(
            f"collections/{self.collection_id}/resources", exist_ok=True)

        self.getSetResources()

    def __str__(self):
        return f'Collection: {self.name}'

    def __repr__(self):
        return f'Collection(\'{self.name}\')'

    def getResources(self):
        print(f"getting resources for collection {self.collection_id}")
        sql = f"select * from '../data_txgio/resources.parquet' where collection_id = '{self.collection_id}'"
        con = duckdb.connect(database=":memory:", read_only=False)
        res = con.execute(sql)
        data = res.df().to_dict(orient="records")

        con.close()

        return data

    def getSetResources(self):
        for r in self.getResources():
            rsc = Resource(r, self.logger)
            self.resources.append(rsc)

        setattr(self, 'resources', self.resources)

        return self.resources

    def downloadAllResourcesThreaded(self, max_workers=5, skip_if_exists=True):
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(resource.download)
                       for resource in self.resources]

            for future in concurrent.futures.as_completed(futures):
                pass

    def deleteAllPartialResourceDownloads(self):
        for r in self.resources:
            r.deletePartialResourceDownload()


class Resource:
    def __init__(self, data: dict, logger: SqliteLogger):
        self.logger = logger

        if isinstance(data, dict):
            for key, value in data.items():
                setattr(self, key, value)
        else:
            raise TypeError("Input data must be a dictionary")

        self.filename = self.resource.split('/')[-1]
        self.unzip_folder_name = self.filename.split('.')[0]
        self.filepath = f"collections/{self.collection_id}/resources/{self.filename}"
        self.unzip_folder_path = f"collections/{self.collection_id}/resources/{self.unzip_folder_name}"
        self.remote_s3_path = f"cng/{self.unzip_folder_path}"
        self.cng_path = f"cng/{self.unzip_folder_name}"

    def download(self, skip_if_exists=True):
        if skip_if_exists and (os.path.exists(self.filepath) or os.path.exists(self.unzip_folder_path)):
            print(
                f"Resource {self.resource} already downloaded {'and unzipped' if os.path.exists(self.unzip_folder_path) else ''} to path {self.filepath if os.path.exists(self.filepath) else self.unzip_folder_path}. Skipping...")
            return

        try:
            response = requests.get(self.resource, stream=True)
            response.raise_for_status()
            with open(self.filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(
                f"Downloaded {self.resource} to {self.filepath}")
        except requests.exceptions.RequestException as e:
            print(
                f"Error downloading {self.resource} to path {filepath}"
            )
        return

    def unzip(self):
        os.makedirs(self.unzip_folder_path, exist_ok=True)

        try:
            with zipfile.ZipFile(self.filepath, 'r') as zip_ref:
                zip_ref.extractall(self.unzip_folder_path)

            print(f"Successfully extracted to {self.unzip_folder_path}")
            print(f"Removing unzipped file {self.filepath}")
            self.delete(self.filepath)
        except FileNotFoundError:
            print(f"Error: Zip file not found at {self.filepath}")
        except zipfile.BadZipfile:
            print(f"Error: Invalid zip file at {self.filepath}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def delete(self, filepath):
        try:
            os.remove(filepath)
            print(f"File {filepath} deleted successfully.")
        except FileNotFoundError:
            print(f"File {filepath} not found.")
        except OSError as e:
            print(f"Error: Could not delete file {filepath}. {e}...")

    def deletePartialResourceDownload(self):
        if os.path.exists(self.filepath):
            local_size = os.path.getsize(self.filepath)
            remote_size = self.filesize

            if remote_size > local_size:
                print(
                    f"Attempting to delete {self.filepath}. {remote_size} > {local_size}...")
                self.delete(self.filepath)

    def translateToCloudNative(self):
        pass

    def get_resource_files_to_translate(self):
        extensions = ["laz", "tiff", "geotiff", "tif", "shp", "gdb"]
        files = [file for file in os.listdir(
            self.unzip_folder_path) if file.split('.')[-1] in extensions or file.split('.')[-1] == file]
        # files = os.listdir(self.unzip_folder_path)
        print(files)

    def upsert_resource_log_row(self):
        fp = self.filepath if os.path.exists(self.filepath) else None
        unzippath = self.unzip_folder_path if os.path.exists(
            self.unzip_folder_path) else None
        cngpath = self.cng_path if os.path.exists() else None

        sql = f"UPSERT INTO {self.log_table} VALUES ({self.resource, fp, unzippath, cngpath, None}) RETURNING *;"
        con = self.logger.get_connection()

        print(con.execute(sql).fetchdf())

        con.close()

    def get_resource_log_row(self, resourceurl):
        sql = f"SELECT * FROM {self.log_table} WHERE resourceurl='{resourceurl}'"
        con = self.logger.get_connection()

        print(con.execute(sql).fetchdf())

        con.close()


sqlite_logger = SqliteLogger()

for c in collections:
    i = Collection(c, sqlite_logger)
    # i.downloadAllResourcesThreaded()
    i.deleteAllPartialResourceDownloads()

    for r in i.resources:
        if os.path.exists(r.filepath) and not os.path.exists(r.unzip_folder_path):
            r.unzip()

            # catalog = pystac.Catalog(
            #    "txgio", description="A catalog of data acquired and maintained by the Texas Geographic Information Office.")

            # print(catalog.description)

        if not os.path.exists(r.filepath) and not os.path.exists(r.unzip_folder_path):
            # typically we would want to download this file using r.download()
            print(
                f"Resource {r.resource} not downloaded or unzipped locally yet. Skipping...")

            pass

        if os.path.exists(r.unzip_folder_path):
            print(
                f"Resource {r.resource} already downloaded and unzipped at {r.unzip_folder_path}...")

    for r in i.resources:
        if os.path.exists(r.unzip_folder_path):
            r.get_resource_files_to_translate()
        else:
            print(f" {r.unzip_folder_path} DNE")
