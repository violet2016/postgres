-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION metrics_collector" to load this file. \quit

DO $$ BEGIN
IF NOT current_setting('shared_preload_libraries') LIKE '%metrics_collector%' THEN
        RAISE EXCEPTION 'extension can only be created when shared_preload_libraries contains metrics_collector';
END IF;
-- IF current_database() != 'gpperfmon' THEN
--	RAISE EXCEPTION 'metrics_collector can only be loaded in the "gpperfmon" database';
--END IF;
END $$ ;

CREATE SCHEMA IF NOT EXISTS @extschema@;

CREATE OR REPLACE FUNCTION @extschema@.metrics_collector_start_worker()
RETURNS void 
AS 'MODULE_PATHNAME', 'metrics_collector_start_worker'
LANGUAGE C STRICT;

SELECT @extschema@.metrics_collector_start_worker();
