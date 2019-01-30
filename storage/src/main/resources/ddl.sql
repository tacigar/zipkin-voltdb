CREATE TABLE span
(
  trace_id VARCHAR(32) NOT NULL,
  span_id VARCHAR(16) NOT NULL,
  ts TIMESTAMP,
  md5 VARBINARY(16) NOT NULL,
  json VARCHAR NOT NULL,
  PRIMARY KEY (trace_id, span_id, md5)
);

-- Partition this table to get parallelism.
PARTITION TABLE span ON COLUMN trace_id;

CREATE PROCEDURE storeSpanJson PARTITION ON TABLE span COLUMN trace_id PARAMETER 0 AS
  INSERT INTO span (trace_id, span_id, ts, md5, json) VALUES (?, ?, TO_TIMESTAMP(Micros, ?), ?, ?);

CREATE PROCEDURE getSpanJson PARTITION ON TABLE span COLUMN trace_id PARAMETER 0 AS
  SELECT json from span where trace_id = ?;

CREATE PROCEDURE getSpansJson AS
  SELECT json from span where ts BETWEEN TO_TIMESTAMP(Millis, ?) AND TO_TIMESTAMP(Millis, ?);
