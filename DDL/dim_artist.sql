CREATE TABLE source.dim_artist (
  artist_id varchar primary key UNIQUE,
  name varchar,
  genres varchar[],
  popularity integer
);
