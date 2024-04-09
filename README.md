# Duckdb_DataAnalysis_ListenBrainz

# A sample repository to showcase ingestion of publicly available listenbrainz data and perform analysis using duckdb. 

# Data is already inserted into ListenBrainz.db in a schema

# Environment recommended

GitHub<br>
Gitpod<br>
VisualstudioCode<br>

# Requirements

duckdb<br>
pandas<br>
matplotlib<br>
Git LFS (if we are storing txt file in the repo)<br>


# Files and their functionality

1 ./DataIngestion/LoadDataset.py --> This is used setup a connection to duckdb and ingest listenbrainz data into a table. A data model can be defined further to break down this data into multiple tables using dimensional modelling. For this task we can achieve the results using sql queries.<br>

2. ./DataAnalysis/a_summary.py --> This is used to highlight first look at the data. Understand the granularity and display summary based on ListenBrainz.db(duckdb) that is storing the data integrated in above step.<br>

3. ./DataAnalysis/b_features.py --> This is used to share information that is needed for clustering of users by providing important features <br>

4. ./DataAnalysis/c_UserActivity_Analysis.py --> This is used to answer few important questions by running queries against duckdb database.<br>

5. ./DataAnalysis/Report.py --> This is used calculate important metrics from the data by using sql commmands on duckdb and visualise the same using graphs. <br>

   


Task 3 : Report <br>

output can be seen from the file ./DataAnalysis/Report.png

