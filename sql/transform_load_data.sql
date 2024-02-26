-- DATA CLEANING

-- maker column

-- genmodel column

-- genmodel_id column

-- adv_id column

--adv_year column
ALTER TABLE ad_table
ALTER COLUMN adv_year
TYPE INTEGER
USING adv_year::INTEGER;

--adv_month column
ALTER TABLE ad_table
ALTER COLUMN adv_month
TYPE INTEGER
USING adv_month::INTEGER;

SELECT *
FROM ad_table
WHERE adv_month NOT BETWEEN 0 AND 12;

DELETE FROM ad_table
WHERE adv_month NOT BETWEEN 0 AND 12;

-- color column

-- reg_year column
SELECT *
FROM ad_table
WHERE reg_year IS NULL;

DELETE FROM ad_table
WHERE reg_year IS NULL;

-- body_type column
SELECT *
FROM ad_table
WHERE body_type = 'Manual';

UPDATE ad_table
SET
    gearbox = 'Manual',
    body_type = NULL
WHERE body_type = 'Manual';

-- run_miles column
SELECT *
FROM ad_table
WHERE run_miles !~ '^\d+(\.\d+)?$';


UPDATE ad_table
SET run_miles = 1
WHERE run_miles = '1 mile';

SELECT adv_id, ROUND(CAST(run_miles AS FLOAT) / (DATE_PART('year', CURRENT_DATE) - reg_year + 1))
FROM ad_table
WHERE run_miles IS NOT NULL
ORDER BY 2 DESC;

DELETE FROM ad_table
WHERE adv_id = '95_15$$993';

ALTER TABLE ad_table
ALTER COLUMN run_miles
TYPE INTEGER
USING run_miles::INTEGER;

-- engine_size column
SELECT *
FROM ad_table
WHERE engine_size !~ 'L$';

UPDATE ad_table
SET engine_size = REGEXP_REPLACE(engine_size, 'L$', '');

ALTER TABLE ad_table
ALTER COLUMN engine_size
TYPE FLOAT
USING engine_size::FLOAT;

-- gearbox column

-- fuel_type column
SELECT DISTINCT fuel_type
FROM ad_table;

UPDATE ad_table
SET fuel_type =
    CASE
        WHEN fuel_type = 'Hybrid  Petrol/Electric' THEN 'Petrol Hybrid'
        WHEN fuel_type = 'Hybrid  Petrol/Electric Plug-in' THEN 'Petrol Plug-in Hybrid'
        WHEN fuel_type = 'Hybrid  Diesel/Electric' THEN 'Diesel Hybrid'
        WHEN fuel_type = 'Hybrid  Diesel/Electric Plug-in' THEN 'Diesel Plug-in Hybrid'
        WHEN fuel_type = 'Petrol Ethanol' THEN 'Petrol'
        ELSE fuel_type
    END;

-- price column
SELECT DISTINCT price
FROM ad_table
WHERE price !~ '^\d+(\.\d+)?$';

DELETE FROM ad_table
WHERE price = 'Uknown';

ALTER TABLE ad_table
ALTER COLUMN price
TYPE INTEGER
USING price::INTEGER;

SELECT price
FROM ad_table
ORDER BY 1 DESC;

DELETE FROM ad_table
WHERE price = 9999999;

-- seat_num
SELECT *
FROM ad_table
WHERE seat_num = 1;

DELETE FROM ad_table
WHERE seat_num = 1;

ALTER TABLE ad_table
ALTER COLUMN seat_num
TYPE INTEGER
USING seat_num::INTEGER;

-- door_num
SELECT *
FROM ad_table
WHERE door_num = 0;

DELETE FROM ad_table
WHERE door_num = 0;

ALTER TABLE ad_table
ALTER COLUMN door_num
TYPE INTEGER
USING door_num::INTEGER;

-- DATA TRANSFORMATION AND LOADING
INSERT INTO dim_body (body_type)
SELECT DISTINCT body_type
FROM ad_table
WHERE body_type NOT IN (
    SELECT body_type
    FROM dim_body
);

INSERT INTO dim_color (color)
SELECT DISTINCT color
FROM ad_table
WHERE color NOT IN (
    SELECT color
    FROM dim_color
);

INSERT INTO dim_date (adv_year, adv_month)
SELECT DISTINCT adv_year, adv_month
FROM ad_table
WHERE (adv_year, adv_month) NOT IN (
    SELECT adv_year, adv_month
    FROM dim_date
);

INSERT INTO dim_fuel (fuel_type)
SELECT DISTINCT fuel_type
FROM ad_table
WHERE fuel_type NOT IN (
    SELECT fuel_type
    FROM dim_fuel
);

INSERT INTO dim_gearbox (gearbox)
SELECT DISTINCT gearbox
FROM ad_table
WHERE gearbox NOT IN (
    SELECT gearbox
    FROM dim_gearbox
);

INSERT INTO dim_maker (maker)
SELECT DISTINCT maker
FROM ad_table
WHERE maker NOT IN (
    SELECT maker
    FROM dim_maker
);

INSERT INTO dim_model (genmodel, genmodel_id)
SELECT DISTINCT genmodel, genmodel_id
FROM ad_table
WHERE (genmodel, genmodel_id) NOT IN (
    SELECT genmodel, genmodel_id
    FROM dim_model
);



INSERT INTO fact_car_adv (
    adv_id,
    key_date,
    key_maker,
    key_model,
    key_body,
    key_color,
    key_gearbox,
    key_fuel,
    reg_year,
    engine_size,
    seat_num,
    door_num,
    run_miles,
    price
)
SELECT
    s.adv_id,
    dim_date.key_date,
    dim_maker.key_maker,
    dim_model.key_model,
    dim_body.key_body,
    dim_color.key_color,
    dim_gearbox.key_gearbox,
    dim_fuel.key_fuel,
    s.reg_year,
    s.engine_size,
    s.seat_num,
    s.door_num,
    s.run_miles,
    s.price
FROM ad_table s
    JOIN dim_body ON COALESCE(dim_body.body_type, '') = COALESCE(s.body_type, '')
    JOIN dim_color ON COALESCE(dim_color.color, '') = COALESCE(s.color, '')
    JOIN dim_date ON COALESCE(dim_date.adv_year, -1) = COALESCE(s.adv_year, -1)
        AND COALESCE(dim_date.adv_month, -1) = COALESCE(s.adv_month, -1)
    JOIN dim_fuel ON COALESCE(dim_fuel.fuel_type, '') = COALESCE(s.fuel_type, '')
    JOIN dim_gearbox ON COALESCE(dim_gearbox.gearbox, '') = COALESCE(s.gearbox, '')
    JOIN dim_maker ON COALESCE(dim_maker.maker, '') = COALESCE(s.maker, '')
    JOIN dim_model ON COALESCE(dim_model.genmodel, '') = COALESCE(s.genmodel, '')
        AND COALESCE(dim_model.genmodel_id, '') = COALESCE(s.genmodel_id, '')
    LEFT JOIN fact_car_adv USING (adv_id)
WHERE fact_car_adv.adv_id IS NULL;