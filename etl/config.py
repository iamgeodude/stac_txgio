from string import Template


def main():
    pass


if __name__ == "__main__":
    main()


COLLECTIONS = ['447db89a-58ee-4a1b-a61f-b918af2fb0bb',
               '6d9c4a2e-b5bb-49b3-9ceb-0727f4711c5b', '3e1530dc-d5be-4683-811b-9d4b162f0aa9']

TXGIO_RESOURCES_PARQUET = "../../data_txgio/resources.parquet"
TXGIO_COLLECTIONS_PARQUET = "../../data_txgio/collections.parquet"

TXGIO_ASSET_ZIPPED_BASEPATH = "unprocessed_data/zip"
TXGIO_ASSET_UNZIP_BASEPATH = "unprocessed_data/unzip"
TXGIO_ASSET_CNG_BASEPATH = "processed_data/catalog"
TXGIO_ASSET_ZIPPED_PATH = Template(
    "${basepath}/${collection_id}/${resource_id}.zip")
TXGIO_ASSET_UNZIP_PATH = Template(
    "${basepath}/${collection_id}/${resource_id}")
TXGIO_ASSET_CNG_PATH = Template(
    "${basepath}/${collection_id}/items")

TXGIO_LOG_BASEPATH = "logs"
TXGIO_RESOURCE_DL_LOG = f"{TXGIO_LOG_BASEPATH}/resources_downloaded.csv"
TXGIO_RESOURCE_UNZIP_LOG = f"{TXGIO_LOG_BASEPATH}/resources_unzipped.csv"
TXGIO_RESOURCE_ASSET_TRANSLATE_LOG = f"{TXGIO_LOG_BASEPATH}/assets_translated.csv"
TXGIO_CNG_ASSET_UPLOADED_LOG = f"{TXGIO_LOG_BASEPATH}/cng_assets_uploaded.csv"
