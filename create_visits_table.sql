-- DDL generated by DBeaver
-- WARNING: It may differ from actual native database DDL
CREATE TABLE splunk.ods_visits (
	visit_id text NOT NULL PRIMARY KEY,
	hit_time timestamp NOT NULL,
	visit_duration numeric(8) NULL,
	object_id text NULL,
	object_name text NULL,
	object_type text NULL,
	object_method text NULL,
	object_system text NULL,
	action_name text NULL,
	area text NULL,
	category text NULL,
	author_id text NULL,
	author_name text NULL,
	author_type text NULL,
	url text NULL,
	is_bot bool NULL,
	session_id int4 NULL REFERENCES public.sessions(session_id),
	"_deleted" text NULL,
	reporting_period date NULL,
	kpi_type text NULL
);

GRANT ALL ON TABLE splunk.ods_visits TO data_app;
