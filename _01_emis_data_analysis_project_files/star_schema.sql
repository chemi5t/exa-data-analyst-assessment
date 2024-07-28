-- Create the database schema 
-- Task 1: Casting columns to the correct data_types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::UUID),
    ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
    ALTER COLUMN card_number TYPE VARCHAR(255) USING (card_number::VARCHAR(255)), 
    ALTER COLUMN store_code TYPE VARCHAR(255) USING (store_code::VARCHAR(255)), 
    ALTER COLUMN product_code TYPE VARCHAR(255) USING (product_code::VARCHAR(255)), 
    ALTER COLUMN product_quantity TYPE SMALLINT USING (product_quantity::SMALLINT);

-- Task 2: Casting columns to the correct data_types
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255) USING (first_name::VARCHAR(255)),
    ALTER COLUMN last_name TYPE VARCHAR(255) USING (last_name::VARCHAR(255)),
    ALTER COLUMN date_of_birth TYPE DATE USING (date_of_birth::DATE),
    ALTER COLUMN country_code TYPE VARCHAR(255) USING (country_code::VARCHAR(255)), 
    ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
    ALTER COLUMN join_date TYPE DATE USING (join_date::DATE);

-- Task 3:  Casting columns to the correct data_types
-- SELECT *
--     FROM dim_store_details
--     WHERE longitude = 'N/A';

UPDATE dim_store_details
    SET address = NULL
    WHERE address = 'N/A';

UPDATE dim_store_details
    SET longitude = NULL
    WHERE longitude = 'N/A';

UPDATE dim_store_details
    SET locality = NULL
    WHERE locality = 'N/A';

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING (longitude::FLOAT),
    ALTER COLUMN locality TYPE VARCHAR(255) USING (locality::VARCHAR(255)),
    ALTER COLUMN store_code TYPE VARCHAR(255) USING (store_code::VARCHAR(255)), 
    ALTER COLUMN staff_numbers TYPE SMALLINT USING (staff_numbers::SMALLINT),
    ALTER COLUMN opening_date TYPE DATE USING (opening_date::DATE),
    ALTER COLUMN store_type TYPE VARCHAR(255) USING (store_type::VARCHAR(255)),  -- and NULLABLE ?
    ALTER COLUMN latitude TYPE FLOAT USING (latitude::FLOAT),
    ALTER COLUMN country_code TYPE VARCHAR(255) USING (country_code::VARCHAR(255)), 
    ALTER COLUMN continent TYPE VARCHAR(255) USING (continent::VARCHAR(255));

-- Task 4:  Altering table to take weight_class column
UPDATE dim_products  
    SET product_price = REPLACE(product_price, '£', '')  -- Step 1: Remove '£' character from 'product_price' values
    WHERE product_price LIKE '£%';

ALTER TABLE dim_products 
    RENAME COLUMN product_price TO "product_price_(gbp)";  -- Step 2: Rename 'product_price' to 'product_price_(gbp)''
    
ALTER TABLE dim_products 
    ADD COLUMN IF NOT EXISTS weight_class VARCHAR(50);  -- Step 3: Add a new column 'weight_class'

UPDATE dim_products  -- Step 4: Update weight_class based on weight ranges
    SET weight_class = 
        CASE 
            WHEN "weight_(kg)" >= 0 AND "weight_(kg)" < 5 THEN 'Light'
            WHEN "weight_(kg)" >= 5 AND "weight_(kg)" < 10 THEN 'Medium'
            WHEN "weight_(kg)" >= 10 AND "weight_(kg)" < 20 THEN 'Heavy'
            ELSE 'Very Heavy'
        END;

-- Task 5:  Casting columns to the correct data_types
ALTER TABLE dim_products  -- Step 1: Rename 'removed' to 'still_available'
    RENAME COLUMN removed TO still_available;
ALTER TABLE dim_products  -- Step 2: Change data types of columns
    ALTER COLUMN "product_price_(gbp)" TYPE FLOAT USING ("product_price_(gbp)"::FLOAT);
ALTER TABLE dim_products  
    ALTER COLUMN "weight_(kg)" TYPE FLOAT USING ("weight_(kg)"::FLOAT);
ALTER TABLE dim_products
    ALTER COLUMN "EAN" TYPE VARCHAR(255) USING ("EAN"::VARCHAR(255));
ALTER TABLE dim_products 
    ALTER COLUMN product_code TYPE VARCHAR(255) USING (product_code::VARCHAR(255));
ALTER TABLE dim_products
    ALTER COLUMN date_added TYPE DATE USING (date_added::DATE);
ALTER TABLE dim_products 
    ALTER COLUMN uuid TYPE UUID USING (uuid::UUID);
ALTER TABLE dim_products
    ALTER COLUMN weight_class TYPE VARCHAR(255) USING (weight_class::VARCHAR(255));

-- ALTER TABLE dim_products
--     ALTER COLUMN still_available TYPE boolean USING (still_available = 'Still_available'::text::boolean);
ALTER TABLE dim_products
    ALTER COLUMN still_available TYPE BOOLEAN USING
    CASE
        WHEN still_available ILIKE 'Still_available' THEN TRUE
        ELSE FALSE
    END;

-- Task 6:  Casting columns to the correct data_types
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(255) USING (month::VARCHAR(255)),  -- Step 1: Change data types of columns
    ALTER COLUMN year TYPE VARCHAR(255) USING (year::VARCHAR(255)),
    ALTER COLUMN day TYPE VARCHAR(255) USING (day::VARCHAR(255)),
    ALTER COLUMN time_period TYPE VARCHAR(255) USING (time_period::VARCHAR(255)),
    ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::UUID);

-- Task 7:  Casting columns to the correct data_types
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(255) USING (card_number::VARCHAR(255)),  -- Step 1: Change data types of columns
    ALTER COLUMN expiry_date TYPE VARCHAR(255) USING (expiry_date::VARCHAR(255)),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING (date_payment_confirmed::DATE);

-- Task 8
-- Create primary key in the dimensions tables
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid); 
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

-- Task 9
-- Create foreign key constraints to finalise the star-schema
ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_date FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
    ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_uuid)  REFERENCES dim_users(user_uuid),
    ADD CONSTRAINT fk_orders_store FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
    ADD CONSTRAINT fk_orders_product FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
    ADD CONSTRAINT fk_orders_card FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);