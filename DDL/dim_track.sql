CREATE TABLE dim_track (
  track_id varchar primary key,
  artist_id varchar,
  artist_ids varchar[],
  name varchar,
  duration_ms integer,
  explicit bool,
  popularity integer,
  album_id varchar,
  danceability numeric,
  energy numeric,
  key integer,
  loudness numeric,
  mode integer,
  speechiness numeric,
  acousticness numeric,
  instrumentalness numeric,
  liveness numeric,
  tempo numeric,
  time_signature integer
);