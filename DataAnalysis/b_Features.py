import duckdb
import os
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))

duckdb_file_path = os.path.join(current_dir, '..', 'DataIngestion', 'ListenBrainz.db')


def CreateConnection():
    con = duckdb.connect(duckdb_file_path)
    return con


ddb_con = CreateConnection()
# a table with features for every user
result = ddb_con.sql("Select user_name As user_id,track_name,artist_name,release_name, to_timestamp(listened_at) As Listened_At,listening_from from Tracks").to_df()

df_features = pd.DataFrame(result)

# print features dataframe
print(df_features)


ddb_con.close()