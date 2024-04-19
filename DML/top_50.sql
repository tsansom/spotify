CREATE OR REPLACE VIEW reporting.top_50 AS
SELECT
	track.name AS track_name,
	artist.name AS artist_name,
	album.name AS album_name,
	ft.rank,
	ft.time_range,
	track.duration_ms / 1000 AS duration_s,
	track.popularity AS track_popularity,
	artist.popularity AS artist_popularity,
	album.popularity AS album_popularity,
	track.danceability,
	track.energy,
	track.loudness,
	track.mode,
	track.speechiness,
	track.acousticness,
	track.instrumentalness,
	track.liveness,
	track.tempo,
	track.time_signature,
	track.valence,
	dsk.key_alpha AS song_key,
	album.release_date,
	artist.genres AS artist_genres
FROM fact_top_50 ft
LEFT JOIN dim_track track USING (track_id)
LEFT JOIN dim_artist artist USING (artist_id)
LEFT JOIN dim_album album USING (album_id)
LEFT JOIN dim_song_key dsk USING (key)
WHERE is_current;
