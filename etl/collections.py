import duckdb
import pandas as pd
import config


def main():
    pass


if __name__ == "__main__":
    main()


def get_collections() -> pd.DataFrame:
    con = duckdb.connect(database=":memory:", read_only=False)
    coll_str = ""
    for x in config.COLLECTIONS:
        coll_str += f"'{x}',"
    sql = f"select * from '../data_txgio/collections.parquet' where collection_id in ({coll_str})"
    print(f"GETTING COLLECTIONS: {sql}")

    res = con.execute(sql)
    data = res.df()

    con.close()

    return data
