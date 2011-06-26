SET search_path=public; 

DROP TYPE IF EXISTS node_role CASCADE;
CREATE TYPE node_role AS ENUM ('O', 'R', 'A', 'D');

DROP TABLE IF EXISTS pg_sched CASCADE;
CREATE TABLE pg_sched
(
  id bigserial NOT NULL PRIMARY KEY,
  datname text NOT NULL,
  proname text NOT NULL,
  pronamespace text NOT NULL,
  proargs text[],
  proargtypes text[],
  usename text,
  frequency interval NOT NULL,
  frequency_offset interval NOT NULL DEFAULT '0s',
  last_run timestamp,
  enabled node_role NULL DEFAULT 'O', 
  isexclusive boolean NOT NULL DEFAULT true,
  isloose boolean NOT NULL DEFAULT true,
  tag varchar(50),  
  running integer,
  CONSTRAINT pg_sched_unique UNIQUE (datname, proname, pronamespace, proargs, proargtypes, frequency, frequency_offset, enabled),
  CHECK (array_length(proargs,1) IS NOT DISTINCT FROM array_length(proargtypes,1))
);

CREATE OR REPLACE FUNCTION pg_sched_check_types() RETURNS TRIGGER AS
$BODY$
BEGIN
	IF EXISTS(SELECT 1 FROM unnest(NEW.proargtypes) a
			LEFT JOIN pg_catalog.pg_type 
			ON pg_catalog.format_type(oid, NULL) = a
				OR typname = a WHERE typname IS NULL) THEN
		RAISE EXCEPTION 'Value for column "proargtypes" contains invalid types: %', NEW.proargtypes;
	END IF;
	RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

REVOKE ALL ON pg_sched FROM public;
GRANT SELECT ON pg_sched TO public;

CREATE TRIGGER pg_sched_check_types_trigger
BEFORE UPDATE OR INSERT ON pg_sched
FOR EACH ROW WHEN (NEW.proargtypes IS NOT NULL)
EXECUTE PROCEDURE pg_sched_check_types();

-- Superuser is required to run tasks under differing credentials
-- CREATE USER pg_sched WITH PASSWORD 'my password' SUPERUSER ; 
-- ALTER TABLE pg_sched OWNER TO pg_sched;

-- Example task
-- INSERT INTO pg_sched (usename, datname, pronamespace, proname, proargs, proargtypes, enabled, frequency, frequency_offset) 
-- VALUES ('test_user', 'mydb', 'pg_catalog', 'date_part', '{"hour", "2015-03-25 13:50:59"}', '{text, timestamp}', 'A', '1 hour', '1 minute');
