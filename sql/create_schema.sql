CREATE TABLE dim_maker (
    key_maker INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    maker VARCHAR
);

CREATE TABLE dim_body (
    key_body INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    body_type VARCHAR
);

CREATE TABLE dim_gearbox (
    key_gearbox INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    gearbox VARCHAR
);

CREATE TABLE dim_fuel (
    key_fuel INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    fuel_type VARCHAR
);

CREATE TABLE dim_color (
    key_color INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    color VARCHAR
);

CREATE TABLE dim_model (
    key_model INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    genmodel VARCHAR,
    genmodel_id VARCHAR
);

CREATE TABLE dim_date (
    key_date INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    adv_year INTEGER,
    adv_month INTEGER
);

CREATE TABLE fact_car_adv (
    adv_id VARCHAR PRIMARY KEY,
    key_date INTEGER,
    key_maker INTEGER,
    key_model INTEGER,
    key_body INTEGER,
    key_color INTEGER,
    key_gearbox INTEGER,
    key_fuel INTEGER,
    reg_year INTEGER,
    engine_size FLOAT,
    seat_num INTEGER,
    door_num INTEGER,
    run_miles INTEGER,
    price INTEGER,
    CONSTRAINT fk_date FOREIGN KEY(key_date) REFERENCES dim_date(key_date),
    CONSTRAINT fk_maker FOREIGN KEY(key_maker) REFERENCES dim_maker(key_maker),
    CONSTRAINT fk_model FOREIGN KEY(key_model) REFERENCES dim_model(key_model),
    CONSTRAINT fk_body FOREIGN KEY(key_body) REFERENCES dim_body(key_body),
    CONSTRAINT fk_color FOREIGN KEY(key_color) REFERENCES dim_color(key_color),
    CONSTRAINT fk_gearbox FOREIGN KEY(key_gearbox) REFERENCES dim_gearbox(key_gearbox),
    CONSTRAINT fk_fuel FOREIGN KEY(key_fuel) REFERENCES dim_fuel(key_fuel)
)
