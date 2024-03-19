import duckdb
import os
import matplotlib.pyplot as plt
import numpy as np


current_dir = os.path.dirname(os.path.abspath(__file__))

duckdb_file_path = os.path.join(current_dir, '..', 'DataIngestion', 'ListenBrainz.db')


def CreateConnection():
    con = duckdb.connect(duckdb_file_path)
    return con


ddb_con = CreateConnection()

# Granularity check
# ddb_con.sql("Select recording_msid ,listened_at ,user_name  ,count(*) from Tracks group by recording_msid ,listened_at ,user_name  having count(*)>1").show()

# following aggregations can answer some questions about the data and the included Spotify users like totallisteninghours, total users etc
result = ddb_con.sql("Select Count(Distinct recording_msid) Total_Tracks,Count(Distinct user_name) Total_Users,sum(duration_ms)/3600000 Total_ListeningTime_InHours, Count(distinct artist_name) Total_Artists,sum(case when Tracks.listening_from = 'spotify' then 1 else 0 END) As Listened_From_Spotify from Tracks").to_df()

plt.figure(figsize=(12, 10))

# Create a grid of tiles
data = result.iloc[0].values.reshape(1, -1)
plt.imshow(data, cmap='Blues', aspect='auto')

# Set tick labels and their positions
plt.xticks(np.arange(len(result.columns)), result.columns, rotation=45)
plt.yticks([])  # No y-axis ticks
plt.title('Summary Metrics')

# Add text annotations
for i in range(len(result.columns)):
    plt.text(i, 0, result.iloc[0, i], ha='center', va='center', color='black')

# Save the plot as an image file
plt.savefig('summary_metrics_tiles.png')