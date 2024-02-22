import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection

import constants as c


def load_df_to_db(df: pd.DataFrame, cols: str | list[str], db_conn: Connection, dim_name: str):
    dim_df = df[cols].drop_duplicates()
    dim_table = pd.read_sql(f'SELECT * FROM {dim_name}', con=db_conn)

    merged_dim = pd.merge(dim_df, dim_table, on=cols, how='outer', indicator=True)

    new_rows = merged_dim[merged_dim['_merge'] == 'left_only'][cols]
    new_rows.to_sql(dim_name, con=db_conn, if_exists='append', index=False)


def merge_dim(df: pd.DataFrame, on_cols: str | list[str], db_conn: Connection, dim_name: str) -> pd.DataFrame:
    dim = pd.read_sql(f'SELECT * FROM {dim_name}', con=db_conn)
    return df.merge(dim, on=on_cols)


def main():
    load_dotenv()
    conn_string = os.getenv(c.CONN_STRING_NAME)

    cars = pd.read_csv(c.SOURCE_FILE)

    db = create_engine(conn_string)
    conn = db.connect()

    load_df_to_db(cars, c.COL_MAKER, conn, c.DIM_MAKER)
    load_df_to_db(cars, c.COL_BODY_TYPE, conn, c.DIM_BODY)
    load_df_to_db(cars, c.COL_GEARBOX, conn, c.DIM_GEARBOX)
    load_df_to_db(cars, c.COL_FUEL_TYPE, conn, c.DIM_FUEL)
    load_df_to_db(cars, c.COL_COLOR, conn, c.DIM_COLOR)
    load_df_to_db(cars, [c.COL_GENMODEL, c.COL_GENMODEL_ID], conn, c.DIM_MODEL)
    load_df_to_db(cars, [c.COL_ADV_YEAR, c.COL_ADV_MONTH], conn, c.DIM_DATE)

    merged = merge_dim(cars, c.COL_MAKER, conn, c.DIM_MAKER)
    merged = merge_dim(merged, c.COL_BODY_TYPE, conn, c.DIM_BODY)
    merged = merge_dim(merged, c.COL_GEARBOX, conn, c.DIM_GEARBOX)
    merged = merge_dim(merged, c.COL_FUEL_TYPE, conn, c.DIM_FUEL)
    merged = merge_dim(merged, c.COL_COLOR, conn, c.DIM_COLOR)
    merged = merge_dim(merged, [c.COL_GENMODEL, c.COL_GENMODEL_ID], conn, c.DIM_MODEL)
    merged = merge_dim(merged, [c.COL_ADV_YEAR, c.COL_ADV_MONTH], conn, c.DIM_DATE)

    wanted_cols = [
        c.COL_ADV_ID,
        c.COL_KEY_DATE,
        c.COL_KEY_MAKER,
        c.COL_KEY_MODEL,
        c.COL_KEY_BODY,
        c.COL_KEY_COLOR,
        c.COL_KEY_GEARBOX,
        c.COL_KEY_FUEL,
        c.COL_REG_YEAR,
        c.COL_ENGINE_SIZE,
        c.COL_SEAT_NUM,
        c.COL_DOOR_NUM,
        c.COL_RUN_MILES,
        c.COL_PRICE
    ]

    existing_ids = set(pd.read_sql(f'SELECT {c.COL_ADV_ID} FROM {c.FACT_CAR_ADV}', con=conn)[c.COL_ADV_ID])

    filtered_merged = merged[~merged[c.COL_ADV_ID].isin(existing_ids)][wanted_cols]

    filtered_merged.to_sql(c.FACT_CAR_ADV, con=conn, if_exists='append', index=False)


if __name__ == "__main__":
    main()
