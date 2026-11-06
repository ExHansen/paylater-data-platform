-- Run once by the postgres image entrypoint (psql) on an empty volume.
-- The entrypoint has already created role pdp (superuser) and database paylater_app.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pdp_cdc') THEN
        CREATE ROLE pdp_cdc WITH LOGIN REPLICATION PASSWORD 'pdp_cdc';
    END IF;
END
$$;

CREATE DATABASE warehouse OWNER pdp;
CREATE DATABASE airflow OWNER pdp;

\connect paylater_app
GRANT CONNECT ON DATABASE paylater_app TO pdp_cdc;
GRANT USAGE ON SCHEMA public TO pdp_cdc;
ALTER DEFAULT PRIVILEGES FOR ROLE pdp IN SCHEMA public GRANT SELECT ON TABLES TO pdp_cdc;
