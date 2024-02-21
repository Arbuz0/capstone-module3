import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


def load_df_to_db(df, columns, db_conn, table_name):
    temp_df = df[columns].drop_duplicates()
    temp_df.to_sql(table_name, con=db_conn, if_exists='append', index=False)


def merge_dim(df, on_cols, db_conn, dim_table):
    dim = pd.read_sql(f'SELECT * FROM {dim_table}', con=db_conn)
    return df.merge(dim, on=on_cols)


def main():
    load_dotenv()
    conn_string = os.getenv("DB_CONNECTION_STRING")
    file_path = os.path.join('..', 'data', 'processed_capstone_data.csv')

    cars = pd.read_csv(file_path)

    db = create_engine(conn_string)
    conn = db.connect()

    load_df_to_db(cars, 'maker', conn, 'dim_maker')
    load_df_to_db(cars, 'body_type', conn, 'dim_body')
    load_df_to_db(cars, 'gearbox', conn, 'dim_gearbox')
    load_df_to_db(cars, 'fuel_type', conn, 'dim_fuel')
    load_df_to_db(cars, 'color', conn, 'dim_color')
    load_df_to_db(cars, ['genmodel', 'genmodel_id'], conn, 'dim_model')
    load_df_to_db(cars, ['adv_year', 'adv_month'], conn, 'dim_date')

    merged = merge_dim(cars, ['maker'], conn, 'dim_maker')
    merged = merge_dim(merged, ['body_type'], conn, 'dim_body')
    merged = merge_dim(merged, ['gearbox'], conn, 'dim_gearbox')
    merged = merge_dim(merged, ['fuel_type'], conn, 'dim_fuel')
    merged = merge_dim(merged, ['color'], conn, 'dim_color')
    merged = merge_dim(merged, ['genmodel', 'genmodel_id'], conn, 'dim_model')
    merged = merge_dim(merged, ['adv_year', 'adv_month'], conn, 'dim_date')

    wanted_cols = [
        'adv_id',
        'key_date',
        'key_maker',
        'key_model',
        'key_body',
        'key_color',
        'key_gearbox',
        'key_fuel',
        'reg_year',
        'engine_size',
        'seat_num',
        'door_num',
        'run_miles',
        'price'
    ]

    filtered_merged = merged[wanted_cols]
    filtered_merged.to_sql('fact_car_adv', con=conn, if_exists='append', index=False)


if __name__ == "__main__":
    main()
