-- Create Airflow database and user for the Airflow services
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow'
   ) THEN
      CREATE ROLE airflow WITH LOGIN PASSWORD 'airflowpass';
   END IF;
END$$;

-- Create 'airflow' database if it does not exist.
-- Note: CREATE DATABASE cannot run inside a function/transaction, so use psql \gexec.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (
   SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER DATABASE airflow OWNER TO airflow;

