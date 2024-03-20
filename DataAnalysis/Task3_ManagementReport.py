import duckdb
import os
import matplotlib.pyplot as plt


current_dir = os.path.dirname(os.path.abspath(__file__))

duckdb_file_path = os.path.join(current_dir, '..', 'DataIngestion', 'ListenBrainz.db')


def CreateConnection():
    con = duckdb.connect(duckdb_file_path)
    return con


ddb_con = CreateConnection()

"""
Most important metrics that I can see from the data are as below
1.Total_TracksPlayed
2.Unique_Tracks_Played
3.Total_ListeningTime_InHours
4.Avg_listening_freq_InMins_byMonth
5.Avg_hour_of_activity
6.Avg_Min_of_activity

"""

ddb_con.sql("Create Or Replace Table Metrics_1 AS select Last_day(to_timestamp(listened_at)) LastDayOfMonth,count(*) Total_TracksPlayed,count(Distinct track_name) As Unique_Tracks_Played, sum(duration_ms)/3600000 Total_ListeningTime_InHours  from Tracks group by Last_day(to_timestamp(listened_at)) order by 1")

ddb_con.sql("Create Or Replace Table Metrics_2 AS With CTE_listeningFreq As (select user_name,Last_day(to_timestamp(listened_at)) LastDayOfMonth, Datediff('min', lag(to_timestamp(listened_at)) over (Partition by user_name Order by to_timestamp(listened_at)), to_timestamp(listened_at)) AS listening_frequency from Tracks order by 2)Select LastDayOfMonth,avg(listening_frequency) Avg_listening_frequency_byMonth from CTE_listeningFreq group by LastDayOfMonth")

ddb_con.sql("Create Or Replace Table Metrics_3 AS With CTE_timeofday AS (select Last_day(to_timestamp(listened_at)) LastDayOfMonth,Avg((EXTRACT(HOUR FROM to_timestamp(listened_at)) * 3600) + (EXTRACT(MINUTE FROM to_timestamp(listened_at)) * 60) + (EXTRACT(SECOND FROM to_timestamp(listened_at)))) As AvgSeconds from Tracks group by 1) Select LastDayOfMonth,Cast(Floor(AvgSeconds / 3600) AS Integer) As Avg_hour_of_activity,Cast(Floor((AvgSeconds % 3600) / 60) As Integer)  AS Avg_min_of_activity  from CTE_timeofday")

result=ddb_con.sql("select M1.LastDayOfMonth,M1.Total_TracksPlayed,M1.Unique_Tracks_Played,M1.Total_ListeningTime_InHours,M2.Avg_listening_frequency_byMonth As Avg_listening_freq_InMins_byMonth,M3.Avg_hour_of_activity,M3.Avg_min_of_activity from Metrics_1 M1 Join Metrics_2 M2 on M1.LastDayOfMonth=M2.LastDayOfMonth Join Metrics_3 M3 on M1.LastDayOfMonth=M3.LastDayOfMonth order by 1  ").to_df()


# Plotting
plt.figure(figsize=(12, 8))

# Plot Total Tracks Played
plt.subplot(3, 2, 1)
plt.plot(result['LastDayOfMonth'], result['Total_TracksPlayed'], marker='o')
plt.title('Total Tracks Played')
plt.xlabel('Date')
plt.ylabel('Total Tracks Played')
plt.xticks(result['LastDayOfMonth'], rotation=45)


# Plot Unique Tracks Played
plt.subplot(3, 2, 2)
plt.plot(result['LastDayOfMonth'], result['Unique_Tracks_Played'], marker='o')
plt.title('Unique Tracks Played')
plt.xlabel('Date')
plt.ylabel('Unique Tracks Played')
plt.xticks(result['LastDayOfMonth'], rotation=45)

# Plot Total Listening Time
plt.subplot(3, 2, 3)
plt.plot(result['LastDayOfMonth'], result['Total_ListeningTime_InHours'], marker='o')
plt.title('Total Listening Time (Hours)')
plt.xlabel('Date')
plt.ylabel('Total Listening Time (Hours)')
plt.xticks(result['LastDayOfMonth'], rotation=45)


# Plot Average Listening Frequency by Month
plt.subplot(3, 2, 4)
plt.plot(result['LastDayOfMonth'], result['Avg_listening_freq_InMins_byMonth'], marker='o')
plt.title('Average Listening Frequency in mins by Month')
plt.xlabel('Date')
plt.ylabel('Average Listening Frequency')
plt.xticks(result['LastDayOfMonth'], rotation=45)

# Plot Average Hour of Activity
plt.subplot(3, 2, 5)
plt.plot(result['LastDayOfMonth'], result['Avg_hour_of_activity'], marker='o')
plt.title('Average Hour of Activity')
plt.xlabel('Date')
plt.ylabel('Average Hour of Activity')
plt.xticks(result['LastDayOfMonth'], rotation=45)

# Plot Average Minute of Activity
plt.subplot(3, 2, 6)
plt.plot(result['LastDayOfMonth'], result['Avg_min_of_activity'], marker='o')
plt.title('Average Minute of Activity')
plt.xlabel('Date')
plt.ylabel('Average Minute of Activity')
plt.xticks(result['LastDayOfMonth'], rotation=45)

plt.tight_layout()
# Save the plot as an image file
plt.savefig('Management_Report.png')



ddb_con.close()

