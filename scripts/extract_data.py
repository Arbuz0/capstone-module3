import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

import constants as c


def prep_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()

    new_names = {
        'bodytype': c.COL_BODY_TYPE,
        'runned_miles': c.COL_RUN_MILES,
        'engin_size': c.COL_ENGINE_SIZE
    }

    df = df.rename(columns=new_names)

    return df


def main():
    load_dotenv()
    conn_string = os.getenv(c.CONN_STRING_NAME)

    cars = pd.read_csv(c.ORIGINAL_FILE)

    db = create_engine(conn_string)
    conn = db.connect()

    cars = prep_df(cars)

    cars.to_sql(c.AD_TABLE, con=conn, if_exists='replace', index=False)


if __name__ == '__main__':
    main()
