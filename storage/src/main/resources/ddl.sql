CREATE TABLE span
(
  trace_id VARCHAR(32) NOT NULL,
  span_id VARCHAR(16) NOT NULL,
  md5 VARBINARY(16) NOT NULL,
  json VARBINARY NOT NULL,
  PRIMARY KEY (trace_id, span_id, md5)
);

-- Partition this table to get parallelism.
PARTITION TABLE span ON COLUMN trace_id;

CREATE PROCEDURE storeSpanJson PARTITION ON TABLE span COLUMN trace_id PARAMETER 0 AS
  INSERT INTO span (trace_id, span_id, md5, json) VALUES (?, ?, ?, ?);

CREATE PROCEDURE getSpanJson PARTITION ON TABLE span COLUMN trace_id PARAMETER 0 AS
  SELECT json from span where trace_id = ?;
