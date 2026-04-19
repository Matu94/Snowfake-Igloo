-------------------------------------------------
-- 1. CREATE ROLE
-------------------------------------------------
CREATE ROLE IF NOT EXISTS demo_role;

-------------------------------------------------
-- 2. CREATE USER WITH PASSWORD
-------------------------------------------------
CREATE USER IF NOT EXISTS demo_user
  PASSWORD = ' '
  DEFAULT_ROLE = demo_role;

GRANT ROLE demo_role TO USER matu;

-- Remove the header/footer lines and newlines so it is one long string
ALTER USER demo_user SET RSA_PUBLIC_KEY ='aaa';


SHOW SCHEMAS IN DATABASE demo_db;
SHOW VIEWS IN SCHEMA staging;
describe table staging.ORDERS_stg;

-------------------------------------------------
-- 3. CREATE DATABASE
-------------------------------------------------
CREATE DATABASE IF NOT EXISTS demo_db;

GRANT USAGE ON DATABASE demo_db TO ROLE demo_role;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE demo_role;

-------------------------------------------------
-- 4. CREATE SCHEMAS
-------------------------------------------------
CREATE SCHEMA IF NOT EXISTS demo_db.raw;
CREATE SCHEMA IF NOT EXISTS demo_db.staging;
CREATE SCHEMA IF NOT EXISTS demo_db.analytics;

GRANT USAGE ON ALL SCHEMAS IN DATABASE demo_db TO ROLE demo_role;
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE, CREATE STREAMLIT ON ALL SCHEMAS IN DATABASE demo_db TO ROLE demo_role;

-------------------------------------------------
-- 5. RAW SCHEMA TABLES
-------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_db.raw.users_raw (
  user_id INTEGER,
  name STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS demo_db.raw.orders_raw (
  order_id INTEGER,
  user_id INTEGER,
  amount NUMBER(10,2),
  order_date DATE
);

-------------------------------------------------
-- 6. STAGING SCHEMA TABLES
-------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_db.staging.users_stg (
  user_id INTEGER,
  name STRING
);

CREATE TABLE IF NOT EXISTS demo_db.staging.orders_stg (
  order_id INTEGER,
  user_id INTEGER,
  amount NUMBER(10,2)
);

-------------------------------------------------
-- 7. STAGING SCHEMA VIEWS
-------------------------------------------------
CREATE OR REPLACE VIEW demo_db.staging.v_users_clean AS
SELECT DISTINCT
  user_id,
  name
FROM demo_db.staging.users_stg;

CREATE OR REPLACE VIEW demo_db.staging.v_orders_clean AS
SELECT
  order_id,
  user_id,
  amount
FROM demo_db.staging.orders_stg
WHERE amount > 0;

-------------------------------------------------
-- 8. ANALYTICS SCHEMA TABLES
-------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_db.analytics.user_metrics (
  user_id INTEGER,
  total_orders INTEGER,
  total_amount NUMBER(10,2)
);

-------------------------------------------------
-- 9. ANALYTICS SCHEMA VIEWS
-------------------------------------------------
CREATE OR REPLACE VIEW demo_db.analytics.v_user_spend AS
SELECT
  user_id,
  COUNT(order_id) AS total_orders,
  SUM(amount) AS total_amount
FROM demo_db.staging.orders_stg
GROUP BY user_id;

-------------------------------------------------
-- 10. GRANTS
-------------------------------------------------
GRANT SELECT ON ALL TABLES IN DATABASE demo_db TO ROLE demo_role;
GRANT SELECT ON ALL VIEWS IN DATABASE demo_db TO ROLE demo_role;
GRANT usage ON ALL stages IN DATABASE demo_db TO ROLE demo_role;

CREATE STAGE MY_APP_STAGE;


CREATE STREAMLIT my_etl_tool
ROOT_LOCATION = '@demo_db.analytics.MY_APP_STAGE'
MAIN_FILE = '/streamlit_app.py'
QUERY_WAREHOUSE = 'COMPUTE_WH';




        
        
