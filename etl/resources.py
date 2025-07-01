import duckdb
import pandas as pd
import config
import requests
import os


def main():
    pass


if __name__ == "__main__":
    main()


def create_dl_log_if_not_exists():
    if not os.path.exists(config.TXGIO_RESOURCE_DL_LOG):
        os.makedirs(config.TXGIO_LOG_BASEPATH, exist_ok=True)

        df = pd.DataFrame({
            'resource_id': [],
            'remote_url': [],
            'collection_id': [],
            'download_path': []
        })
        try:
            df.to_csv(config.TXGIO_RESOURCE_DL_LOG, index=False, header=True)

        except Exception as e:
            print(
                f"\nError creating resource download log at {config.TXGIO_RESOURCE_DL_LOG}:\n\t{e}\n")


def get_resources_by_collection_id(collection_id) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:", read_only=False)

    sql = f"select * from '../data_txgio/resources.parquet' where collection_id='{collection_id}'"
    print(f"GETTING RESOURCES FOR COLLECTION {collection_id}: {sql}")

    res = con.execute(sql)
    data = res.df()

    con.close()

    return data


def get_resource_dl_log():
    """
    checks if logfile exists in logfile path,
    creates if does not exist.
    returns the resulting pandas df.
    """
    create_dl_log_if_not_exists()

    log = pd.read_csv(config.TXGIO_RESOURCE_DL_LOG)

    return log


def is_resource_downloaded(resource_id, remote_url) -> bool:
    """
    checks if resource is in the download log by either resource_id or zipfile name.
    returns true if value present, false if not.
    """
    df = get_resource_dl_log()
    condition = (df['resource_id'].isin([resource_id])) | (
        df['resource'].isin([remote_url]))

    return condition


def log_resource_download(resource_id, remote_url, collection_id, download_path):
    """
    logs when resource download completed successfully
    """

    df = pd.DataFrame({
        'resource_id': [resource_id],
        'remote_url': [remote_url],
        'collection_id': [collection_id],
        'download_path': [download_path]
    })

    df.to_csv(config.TXGIO_RESOURCE_DL_LOG,
              mode='a', header=False, index=False)

    return


def download_resource(resource):
    """
    downloads resource in chunks, providing updates to stdout.

    """
    remote_url = resource["resource"]
    id = resource["resource_id"]
    collection_id = resource["collection_id"]

    download_path = config.TXGIO_ASSET_ZIPPED_PATH.substitute(
        basepath=config.TXGIO_ASSET_ZIPPED_BASEPATH, collection_id=collection_id, resource_id=id)

    # check if resource already in dl log
    if is_resource_downloaded(id, remote_url):
        print(
            f"\nResource with id: {resource['resource_id']}, url: {resource['resource']} already downloaded. Skipping.\n")
        return

    try:
        response = requests.get(remote_url, stream=True)
        response.raise_for_status()

        total_size = int(requests.headers('content-length', 0))
        downloaded_size = 0

        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    progress = (downloaded_size / total_size) * \
                        100 if total_size else 0
                    print(
                        f"\nDownloading {resource_url},\n\t{downloaded_size}/{total_size} bytes,\n\t({progress:2f}%) complete...\n")

            print(f"Download of {remote_url} complete.")

            log_resource_download(id, remote_url, collection_id, download_path)

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {remote_url} to path {download_path}")
    except Exception as e:
        print(
            f"Unexpected error downloading {remote_url} to path {download_path}")

    return
