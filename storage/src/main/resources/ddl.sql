CREATE TABLE Span
(
  trace_id VARCHAR(32) NOT NULL,
  parent_id VARCHAR(16),
  id VARCHAR(16) NOT NULL,
  kind VARCHAR(8),
  service_name VARCHAR(255), -- The localEndpoint.serviceName field in json
  remote_service_name VARCHAR(255), -- The remoteEndpoint.serviceName field in json
  name VARCHAR(255),
  ts TIMESTAMP, -- Derived from the epoch micros timestamp in json
  duration BIGINT, -- The duration field in json, in microseconds
  is_error TINYINT NOT NULL, -- 1 when tags.error exists in json or 0 if not
  md5 VARBINARY(16) NOT NULL, -- MD5 of the json, used to prevent duplicate rows
  json VARCHAR NOT NULL, -- Potentially incomplete v2 json sent by instrumentation
  PRIMARY KEY (trace_id, id, md5)
);

-- Allows procedures to work on a trace as a unit
PARTITION TABLE Span ON COLUMN trace_id;

CREATE PROCEDURE GetSpanJson PARTITION ON TABLE Span COLUMN trace_id PARAMETER 0 AS
  SELECT json from Span where trace_id = ? ORDER BY ts;

CREATE PROCEDURE GetSpanNames AS
  SELECT distinct(name) from Span where service_name = ? or remote_service_name = ? ORDER BY name;

CREATE TABLE DependencyLink
(
  trace_id VARCHAR(32) NOT NULL,
  ts TIMESTAMP NOT NULL, -- first timestamp for the trace
  parent VARCHAR(255) NOT NULL,
  child VARCHAR(255) NOT NULL,
  call_count BIGINT NOT NULL,
  error_count BIGINT NOT NULL,
  PRIMARY KEY (trace_id, parent, child)
);

-- Allows procedures to work on a trace as a unit
PARTITION TABLE DependencyLink ON COLUMN trace_id;
