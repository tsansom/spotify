CREATE TABLE source.fact_recently_played (
  track_id varchar,
  played_at timestamp
);

ALTER TABLE source.fact_recently_played ADD UNIQUE (track_id, played_at);
