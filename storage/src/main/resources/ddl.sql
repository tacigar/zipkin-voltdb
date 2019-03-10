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

CREATE PROCEDURE GetDependencyLinks AS
  SELECT parent, child, SUM(call_count), SUM(error_count) from DependencyLink
   WHERE ts BETWEEN TO_TIMESTAMP(Millis, ?) AND TO_TIMESTAMP(Millis, ?)
   GROUP BY parent, child ORDER BY parent, child;

-- Inserts into Span should imply an upsert here.
-- After a quiet period, rows should be removed and upserted into CompleteTrace
CREATE TABLE PendingTrace
(
  trace_id VARCHAR(32) NOT NULL,
  update_ts TIMESTAMP NOT NULL,
  PRIMARY KEY (trace_id)
);

PARTITION TABLE PendingTrace ON COLUMN trace_id;

-- When a trace has an update, it should be processed and its dirty bit set to zero
-- Sampled traces should be upserted into PendingExport
CREATE TABLE CompleteTrace
(
  trace_id VARCHAR(32) NOT NULL,
  dirty TINYINT NOT NULL, -- 1 if there was an update to this trace, set to 0 once processed
  is_sampled TINYINT NOT NULL, -- 1 if sampled, 0 if not
  PRIMARY KEY (trace_id)
);

PARTITION TABLE CompleteTrace ON COLUMN trace_id;

-- Once exported, rows should be removed. Rows may be re-added on late updates to a trace.
CREATE TABLE PendingExport
(
  trace_id VARCHAR(32) NOT NULL,
  update_ts TIMESTAMP NOT NULL,
  PRIMARY KEY (trace_id)
);

PARTITION TABLE PendingExport ON COLUMN trace_id;