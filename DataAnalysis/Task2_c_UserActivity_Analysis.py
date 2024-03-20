import duckdb
import os


current_dir = os.path.dirname(os.path.abspath(__file__))

duckdb_file_path = os.path.join(current_dir, '..', 'DataIngestion', 'ListenBrainz.db')


def CreateConnection():
    con = duckdb.connect(duckdb_file_path)
    return con



ddb_con = CreateConnection()

# Answers to Questions asked by CEO
print("1.Top 10 most active users")
ddb_con.sql("select user_name,count(*) from Tracks group by user_name order by 2 desc LIMIT 10").show()

print("2.How many users were active on the 1st of March 2019?")
ddb_con.sql("select count(Distinct user_name) from Tracks where Cast(to_timestamp(listened_at) AS DATE)='2019-03-01'").show()

print("3.For every user, what was the first song they listened to?")
ddb_con.sql("WITH CTE_FirstActivity As (select user_name,track_name,row_number() over(Partition by user_name  Order by to_timestamp(listened_at) ) As RowNumber from Tracks) select user_name,track_name As First_Song_Listened_To from CTE_FirstActivity where RowNumber=1 order by user_name;").show()

# Test Results of Q3
# ddb_con.sql("WITH CTE_FirstActivity As (select user_name,track_name,to_timestamp(listened_at) listened_at ,row_number() over(Partition by user_name  Order by to_timestamp(listened_at) ) As RowNumber from Tracks where user_name='AllSparks') select * from CTE_FirstActivity  order by RowNumber ;").show()



ddb_con.close()