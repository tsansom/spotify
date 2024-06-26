from spotify_utils import *
from db_utils import *

'''
Project Notes
-------------

long_term = calculated from several years of data and includes all new data as it becomes available
medium_term = approximately the last 6 months
short_term = approximately the last 4 weeks

a track can have multiple artists
an album can have multiple artists, but not every artist is on every track
the first artist from the track should match the first artist from the album (need to check)
- Since this project is focused on individual tracks not albums, I'll leave the artist ids out of the album table
- it's possible to use the track_ids array to backtrack into the album artists anyways (may need to make this a dim_artist_group table)

Top 50 Flow:
----
The top 50 table will be an SCD (probably type 2 but maybe type 3?)
- Pull the top 50 tracks for the short term
- check to see if we have the tracks in the dim_track table already / load rows if they don't exist
- check to see if we have the artist in dim_artist already / load rows if they don't exist
- check to see if we have the album in dim_album already / load rows if they don't exist
- load top 50 tracks into fact_recently_played
    - check for tracks that already exist in the table in the same rank
    - swap flag for old rows
    - set the valid_to timestamp of the old rows
    - set the flag for the new rows
    - set the valid_from timestamp of the new rows

Recently Played Flow:
----
- Pull recently played tracks (short_term)
- check if the track_id and played_at already exist in the fact_recently_played table
- add new rows to the table if they didn't already exist
- check to see if we have the tracks in the dim_track table already / load rows if they don't exist
- check to see if we have the artist in dim_artist already / load rows if they don't exist
- check to see if we have the album in dim_album already / load rows if they don't exist

Tables:
----
fact_recently_played:
- track_id varchar primary key
- played_at timestamp

fact_top_50:
- track_id varchar
- rank integer
- is_current bool
- valid_from timestamp (?)
- valid_to timestamp (?)
- PRIMARY KEY (track_id, rank)

dim_track:
- track_id varchar primary key
- artist_id varchar
- artist_ids array (may need to extend this to a group table)
- name varchar
- duration_ms integer
- explicit bool
- popularity integer
- album_id varchar
- danceability numeric
- energy numeric
- key integer
- loudness numeric
- mode integer
- speechiness numeric
- acousticness numeric
- instrumentalness numeric
- liveness numeric
- valence numeric
- tempo numeric
- time_signature integer

dim_artist:
- artist_id varchar primary key
- name varchar
- genres array
- popularity integer

dim_album:
- album_id varchar primary key
- name varchar
- popularity integer
- release_date date
- total_tracks integer
- track_ids array
'''

#############################################################################

sp = get_spotify_token()
conn = get_connection()

############################################################################

for time_range in ['short_term', 'medium_term', 'long_term']:
    top50 = get_top_tracks(sp, time_range=time_range)

    top50_tracks = get_track_info(sp, top50)
    top50_albums = get_album_info(sp, top50_tracks)
    top50_artists = get_artist_info(sp, top50_tracks)

    insert_data(conn, top50_tracks, 'source.dim_track')
    insert_data(conn, top50_artists, 'source.dim_artist')
    insert_data(conn, top50_albums, 'source.dim_album')

    insert_scd_source_data(conn, top50)
    update_fact_scd(conn)


recent = get_recently_played(sp)

recent_tracks = get_track_info(sp, recent)
recent_albums = get_album_info(sp, recent_tracks)
recent_artists = get_artist_info(sp, recent_tracks)

insert_data(conn, recent_tracks, 'source.dim_track')
insert_data(conn, recent_albums, 'source.dim_album')
insert_data(conn, recent_artists, 'source.dim_artist')
insert_data(conn, recent, 'source.fact_recently_played')

############################################################################

conn.close()