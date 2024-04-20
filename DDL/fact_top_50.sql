CREATE TABLE FACT_TOP_50 (
  track_id varchar,
  rank integer,
  time_range varchar,
  is_current bool,
  valid_from timestamp
  valid_to timestamp
  PRIMARY KEY (track_id, rank, time_range)
)
