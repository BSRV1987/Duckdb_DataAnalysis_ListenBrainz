import duckdb
import os



current_dir = os.path.dirname(os.path.abspath(__file__))

Json_file_path = os.path.join(current_dir, '..', 'SourceFiles', 'dataset.txt')


# function that setsup connection to database that is stored in the file system
def CreateConnection():
    con = duckdb.connect("ListenBrainz.db")
    return con

# function that reads the export‑file and writes the data into duckdb database
def execute_load(connection,table_name,file_path):
        try:

            CreateStatement = f"""
            CREATE TABLE IF NOT Exists {table_name} ( 
            release_msid UUID, 
            release_mbid VARCHAR, 
            recording_mbid VARCHAR,
            release_group_mbid VARCHAR, 
            artist_mbids JSON, 
            tags JSON,
            work_mbids JSON, 
            isrc VARCHAR, 
            spotify_id VARCHAR,
            tracknumber INT,
            track_mbid VARCHAR,
            artist_msid UUID,
            recording_msid UUID,
            dedup_tag BIGINT,
            artist_names JSON,
            discnumber BigInt,
            duration_ms BigInt,
            listening_from VARCHAR,
            release_artist_name VARCHAR,
            release_artist_names JSON,
            spotify_album_artist_ids JSON,
            spotify_album_id VARCHAR,
            spotify_artist_ids JSON,
            rating VARCHAR,
            source VARCHAR,
            track_length VARCHAR,
            track_number VARCHAR,
            albumartist VARCHAR,
            date VARCHAR,
            totaldiscs INT,
            totaltracks INT,
            choosen_by_user INT,
            duration INT,
            artist_name VARCHAR,
            track_name VARCHAR,
            release_name VARCHAR,
            listened_at BIGINT,
            user_name VARCHAR
            );
            """
                         
            connection.sql(CreateStatement)




            qry=f"""
                Insert into {table_name}(
                release_msid,
                release_mbid,
                recording_mbid,
                release_group_mbid,
                artist_mbids,
                tags,
                work_mbids,
                isrc,
                spotify_id,
                tracknumber,
                track_mbid,
                albumartist,
                artist_msid,
                choosen_by_user,
                duration,
                date,
                rating,
                source,
                track_length,
                track_number,
                artist_names,
                discnumber,
                totaldiscs,
                totaltracks,
                duration_ms,
                listening_from,
                release_artist_name,
                release_artist_names,
                spotify_album_artist_ids,
                spotify_album_id,
                spotify_artist_ids,
                dedup_tag,
                artist_name,
                track_name,
                release_name,
                listened_at,
                user_name,
                recording_msid
                )
            SELECT
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.release_msid'),
                    '"',
                    ''
                ) AS release_msid,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.release_mbid'),
                    '"',
                    ''
                ) AS release_mbid,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.recording_mbid'),
                    '"',
                    ''
                ) AS recording_mbid,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.release_group_mbid'),
                    '"',
                    ''
                ) AS release_group_mbid,
                JSON_EXTRACT(track_metadata, '$.additional_info.artist_mbids') AS artist_mbids,
                JSON_EXTRACT(track_metadata, '$.additional_info.tags') AS tags,
                JSON_EXTRACT(track_metadata, '$.additional_info.work_mbids') AS work_mbids,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.isrc'),
                    '"',
                    ''
                ) AS isrc,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.spotify_id'),
                    '"',
                    ''
                ) AS spotify_id,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.tracknumber'), 
                    '"',
                    ''
                ) AS tracknumber,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.track_mbid'),
                    '"',
                    ''
                ) AS track_mbid,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.albumartist'),
                    '"',
                    ''
                ) AS albumartist,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.artist_msid'),
                    '"',
                    ''
                ) AS artist_msid,
                    JSON_EXTRACT(track_metadata, '$.additional_info.choosen_by_user') AS choosen_by_user,
                    REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.duration'),
                    '"',
                    ''
                ) AS duration,
                    REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.date'),
                    '"',
                    ''
                ) AS date,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.rating'),
                    '"',
                    ''
                ) AS rating,
                    REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.source'),
                    '"',
                    ''
                ) AS source,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.track_length'),
                    '"',
                    ''
                ) AS track_length,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.track_number'),
                    '"',
                    ''
                ) AS track_number,
                JSON_EXTRACT(track_metadata, '$.additional_info.artist_names') AS artist_names,
                JSON_EXTRACT(track_metadata, '$.additional_info.discnumber') AS discnumber,
                JSON_EXTRACT(track_metadata, '$.additional_info.totaldiscs') AS totaldiscs,
                JSON_EXTRACT(track_metadata, '$.additional_info.totaltracks') AS totaltracks,
                JSON_EXTRACT(track_metadata, '$.additional_info.duration_ms') AS duration_ms,
                    REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.listening_from'),
                    '"',
                    ''
                ) AS listening_from,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.release_artist_name'),
                    '"',
                    ''
                ) AS release_artist_name,
                JSON_EXTRACT(track_metadata, '$.additional_info.release_artist_names') AS release_artist_names,
                    JSON_EXTRACT(track_metadata, '$.additional_info.spotify_album_artist_ids') AS spotify_album_artist_ids,
                        REPLACE(
                    JSON_EXTRACT(track_metadata, '$.additional_info.spotify_album_id'),
                    '"',
                    ''
                ) AS spotify_album_id,
                        
                    JSON_EXTRACT(track_metadata, '$.additional_info.spotify_artist_ids') AS spotify_artist_ids,

                JSON_EXTRACT(track_metadata, '$.additional_info.dedup_tag') AS dedup_tag,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.artist_name'),
                    '"',
                    ''
                ) AS artist_name,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.track_name'),
                    '"',
                    ''
                ) AS track_name,
                REPLACE(
                    JSON_EXTRACT(track_metadata, '$.release_name'),
                    '"',
                    ''
                ) AS release_name,
                J.listened_at,
                J.user_name,
                J.recording_msid
            FROM
                read_json_auto('{file_path}', auto_detect=true, sample_size=-1) J
                Left join {table_name} T on J.recording_msid=T.recording_msid and J.listened_at=T.listened_at and J.user_name=T.user_name
                where T.recording_msid is null;
            """
            connection.sql(qry)
        except Exception as e:
            print(f"An error occurred: {e}")
        
     

# con.sql('DESCRIBE Tracks ').show(max_width=10000)

ddb_con = CreateConnection()

# ddb_con.sql("Drop table if exists Tracks")

execute_load(ddb_con,'Tracks',Json_file_path)









