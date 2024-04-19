CREATE TABLE dim_album (
  album_id varchar primary key UNIQUE,
  name varchar,
  popularity integer,
  release_date date,
  total_tracks integer,
  track_ids varchar[]
);
