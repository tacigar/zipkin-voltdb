CREATE TABLE span
(
  trace_id VARCHAR(32) NOT NULL,
  id VARCHAR(16) NOT NULL,
  service_name VARCHAR(255), -- The localEndpoint.serviceName field in json
  name VARCHAR(255),
  ts TIMESTAMP, -- Derived from the epoch micros timestamp in json
  duration BIGINT, -- The duration field in json, in microseconds
  is_error TINYINT NOT NULL, -- 1 when tags.error exists in json or 0 if not
  md5 VARBINARY(16) NOT NULL, -- MD5 of the json, used to prevent duplicate rows
  json VARCHAR NOT NULL, -- Potentially incomplete v2 json sent by instrumentation
  PRIMARY KEY (trace_id, id, md5)
);

-- Allows procedures to work on a trace as a unit
PARTITION TABLE span ON COLUMN trace_id;

CREATE PROCEDURE storeSpanJson PARTITION ON TABLE span COLUMN trace_id PARAMETER 0 AS
  INSERT INTO span (trace_id, id, service_name, name, ts, duration, is_error, md5, json)
    VALUES (?, ?, ?, ?, TO_TIMESTAMP(Micros, ?), ?, ?, ?, ?);

CREATE PROCEDURE getSpanJson PARTITION ON TABLE span COLUMN trace_id PARAMETER 0 AS
  SELECT json from span where trace_id = ?;

CREATE PROCEDURE getSpansJson AS
  SELECT json from span where ts BETWEEN TO_TIMESTAMP(Millis, ?) AND TO_TIMESTAMP(Millis, ?);
