import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import spotipy.util as util
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras

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


def refresh():
    '''
    Refresh the access token - required every hour
    '''
    global token_info, sp

    if sp_oauth.is_token_expired(token_info):
        token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
        token = token_info['access_token']
        sp = spotipy.Spotify(auth=token)

def get_top_tracks(sp, n=50, offset=0, time_range='long_term'):
    '''
    Pull the user top N most played tracks
    '''

    results = sp.current_user_top_tracks(limit=n, offset=offset, time_range=time_range)

    return parse_top_tracks(results, time_range)

def parse_top_tracks(results, time_range):
    '''
    parse the track object from api response and return a dataframe of the results
    - results are returned in rank order (confirmed on https://www.statsforspotify.com/track/top?timeRange=short_term)
    '''

    df = pd.DataFrame(columns=['track_id', 'rank', 'is_current'])

    for rank, result in enumerate(results['items']):
        track_id = result['id']

        df.loc[len(df)] = [track_id, rank+1, True]

    df['time_range'] = time_range

    return df

def get_recently_played(sp, n=50):
    '''
    get the recently played tracks
    '''

    return parse_recently_played(sp.current_user_recently_played(limit=n))

def parse_recently_played(results):
    '''
    parse the recently played tracks to get info and play time
    '''

    df = pd.DataFrame(columns=['track_id', 'played_at'])

    for result in results['items']:
        track_id = result['track']['id']
        played_at = pd.to_datetime(result['played_at'])

        df.loc[len(df)] = [track_id, played_at]

    df['played_at'] = df['played_at'].dt.tz_convert('America/Chicago') \
                                     .dt.tz_localize(None) \
                                     .dt.strftime('%Y-%m-%d %H:%M:%S')

    return df

def get_audio_features(sp, tracks, chunk_size=100):
    '''
    pull the audio features for a track - tracks can be a single track (str) or a list of tracks

    max limit = 100
    '''

    df = pd.DataFrame()

    for i in range(0, len(tracks), chunk_size):
        tmp = parse_audio_features(sp.audio_features(tracks[i:i+chunk_size]))
        df = pd.concat([df, tmp])

    # af = sp.audio_features(tracks)

    return df

def parse_audio_features(results):
    '''
    parse the individual audio features for the track
    '''

    df = pd.DataFrame(columns=['track_id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
                               'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'])

    for result in results:

        track_id = result['id']
        danceability = result['danceability']
        energy = result['energy']
        key = result['key']
        loudness = result['loudness']
        mode = result['mode']
        speechiness = result['speechiness']
        acousticness = result['acousticness']
        instrumentalness = result['instrumentalness']
        liveness = result['liveness']
        valence = result['valence']
        tempo = result['tempo']
        time_signature = result['time_signature']

        df.loc[len(df)] = [track_id, danceability, energy, key, loudness, mode, speechiness, acousticness,
                           instrumentalness, liveness, valence, tempo, time_signature]

    return df

def append_audio_features(sp, df):
    '''
    append track audio features to an existing dataframe of tracks
    '''

    if not df.index.name:
        df = df.set_index('track_id')
    af = get_audio_features(sp, df.index.tolist())
    af_df = af.set_index('track_id')

    return df.join(af_df)

def get_track_info(sp, df, chunk_size=50):
    '''
    get all the track information for the specified list of tracks (max 50)
    '''

    # add a check here to see if the track already exists in dim_track
    # no need to pull if it already exists (will they ever change?)

    tracks = df['track_id'].dropna().unique().tolist()

    df = pd.DataFrame()

    for i in range(0, len(tracks), chunk_size):
        tmp = parse_track_info(sp, sp.tracks(tracks[i:i+chunk_size]))
        df = pd.concat([df, tmp])

    return df

def parse_track_info(sp, results):
    '''
    parse all the data for each track and add the audio features
    '''

    df = pd.DataFrame(columns=['track_id', 'artist_id', 'artist_ids', 'name', 'duration_ms', 'explicit', 'popularity', 'album_id'])

    for result in results['tracks']:
        track_id = result['id']
        artist_ids = [artist['id'] for artist in result['artists']]
        artist_id = result['artists'][0]['id']
        name = result['name']
        duration_ms = result['duration_ms']
        explicit = result['explicit']
        popularity = result['popularity']
        album_id = result['album']['id']

        df.loc[len(df)] = [track_id, artist_id, artist_ids, name, duration_ms, explicit, popularity, album_id]

    df = append_audio_features(sp, df)

    return df

def get_artist_info(sp, df, chunk_size=50):
    '''
    get the unique artists from a dataframe and pull their info (max 50)
    '''

    # add check here to see if artist already exists in dim_artist
    artists = df['artist_id'].dropna().unique().tolist()

    df = pd.DataFrame()

    for i in range(0, len(artists), chunk_size):
        tmp = parse_artist_info(sp.artists(artists[i:i+chunk_size]))
        df = pd.concat([df, tmp])

    return df.set_index('artist_id')

def parse_artist_info(results):
    '''
    parse the results of the artists info pull
    '''

    df = pd.DataFrame(columns=['artist_id', 'name', 'genres', 'popularity'])

    for result in results['artists']:
        artist_id = result['id']
        name = result['name']
        genres = result['genres']
        popularity = result['popularity']

        df.loc[len(df)] = [artist_id, name, genres, popularity]

    return df

def get_album_info(sp, df, chunk_size=20):
    '''
    get the unique albums from a dataframe and pull their info

    maximum size of albums list is 20
    '''

    albums = df['album_id'].dropna().unique().tolist()

    df = pd.DataFrame()

    for i in range(0, len(albums), chunk_size):
        tmp = parse_album_info(sp.albums(albums[i:i+chunk_size]))
        df = pd.concat([df, tmp])


    return df.set_index('album_id')

def parse_album_info(results):
    '''
    parse the results of the albums info pull
    '''

    df = pd.DataFrame(columns=['album_id', 'name', 'popularity', 'release_date', 'total_tracks', 'track_ids'])

    for result in results['albums']:
        album_id = result['id']
        name = result['name']
        popularity = result['popularity']
        release_date = pd.to_datetime(result['release_date'])
        total_tracks = result['total_tracks']
        track_ids = [track['id'] for track in result['tracks']['items']]

        df.loc[len(df)] = [album_id, name, popularity, release_date, total_tracks, track_ids]

    return df

def item_exists(conn, id, target_table):
    cur = conn.cursor()

    if target_table == 'dim_track':
        cur.execute(f"SELECT track_id FROM dim_track WHERE track_id='{id}'")
    elif target_table == 'dim_artist':
        cur.execute(f"SELECT artist_id FROM dim_artist WHERE artist_id='{id}'")
    elif target_table == 'dim_album':
        cur.execute(f"SELECT album_id FROM dim_album WHERE album_id='{id}'")
    else:
        print(f'The table {target_table} does not exist')

    return cur.fetchone() is not None

def insert_data(df, table):
    if df.index.name is not None:
        df = df.reset_index()

    conn = get_connection()
    cur = conn.cursor()
    df_columns = list(df)
    columns = ', '.join(df_columns)
    values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {} ON CONFLICT DO NOTHING".format(table, columns, values)

    psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
    conn.commit()
    conn.close()

def insert_scd_source_data(df, table='staging.fact_top_50_stage'):
    conn = get_connection()
    cur = conn.cursor()

    truncate_stmt = 'TRUNCATE TABLE {};'.format(table)
    cur.execute(truncate_stmt)
    conn.commit()

    df['valid_from'] = pd.to_datetime(datetime.now())
    df['valid_to'] = pd.to_datetime(datetime.strptime('2099-12-31 00:00:00', '%Y-%m-%d %H:%M:%S'))

    df_columns = list(df)
    columns = ', '.join(df_columns)
    values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

    psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
    conn.commit()
    conn.close()

def update_fact_scd(table='source.fact_top_50'):
    q = '''
        WITH u AS (
            UPDATE source.fact_top_50 fact
                SET is_current = False,
                    valid_to = src.valid_from
            FROM staging.fact_top_50_stage src
            WHERE
                fact.track_id != src.track_id
                AND fact.is_current = True
                AND fact.rank = src.rank
                AND fact.time_range = src.time_range
        )

        INSERT INTO source.fact_top_50 (
            track_id,
            rank,
            time_range,
            is_current,
            valid_from,
            valid_to)
    	SELECT
    		a.track_id,
    		a.rank,
    		a.time_range,
    		a.is_current,
    		a.valid_from,
    		a.valid_to
    	FROM staging.fact_top_50_stage AS a
    	LEFT JOIN source.fact_top_50 AS b
    	    ON a.track_id = b.track_id
    	    AND a.rank = b.rank
    	    AND a.time_range = b.time_range
    	    AND b.is_current = True
    	WHERE b.track_id IS NULL
        ON CONFLICT DO NOTHING
    '''

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(q)
    conn.commit()

#############################################################################

scopes = ['user-top-read', 'user-read-recently-played']

sp_oauth = SpotifyOAuth(scope=scopes)
token_info = sp_oauth.get_cached_token()
try:
    token = token_info['access_token']
except:
    # if not token_info:
    auth_url = sp_oauth.get_authorize_url()

    print(auth_url)

    response = input('Paste the above link into your browser, then paste the redirect url here: ')

    code = sp_oauth.parse_response_code(response)
    token_info = sp_oauth.get_access_token(code)

    token = token_info['access_token']

sp = spotipy.Spotify(auth=token)

############################################################################

def get_connection():
    return psycopg2.connect(host='localhost', database='spotify',
                            user='tsansom', password=os.getenv('POSTGRES_PASSWORD'))

############################################################################

for time_range in ['short_term', 'medium_term', 'long_term']:
    top50 = get_top_tracks(sp, time_range=time_range)

    top50_tracks = get_track_info(sp, top50)
    top50_albums = get_album_info(sp, top50_tracks)
    top50_artists = get_artist_info(sp, top50_tracks)

    insert_data(top50_tracks, 'source.dim_track')
    insert_data(top50_artists, 'source.dim_artist')
    insert_data(top50_albums, 'source.dim_album')

    insert_scd_source_data(top50)
    update_fact_scd()


recent = get_recently_played(sp)

recent_tracks = get_track_info(sp, recent)
recent_albums = get_album_info(sp, recent_tracks)
recent_artists = get_artist_info(sp, recent_tracks)

insert_data(recent_tracks, 'source.dim_track')
insert_data(recent_albums, 'source.dim_album')
insert_data(recent_artists, 'source.dim_artist')
insert_data(recent, 'source.fact_recently_played')
