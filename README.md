# Duckdb_DataAnalysis_ListenBrainz

# A sample repository to showcase ingestion of listenbrainz data and perform analysis using duckdb. 

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

2. ./DataAnalysis/Task2_a_summary.py --> This is used to highlight first look at the data. Understand the granularity and display summary based on ListenBrainz.db(duckdb) that is storing the data integrated in above step.<br>

3. ./DataAnalysis/Task2_b_summary.py --> This is used to share information that is needed for clustering of users by providing important features <br>

4. ./DataAnalysis/Task2_c_UserActivity_Analysis.py --> This is used to answer few important questions by running queries against duckdb database.<br>

5. ./DataAnalysis/Task3_ManagementReport.py --> This is used calculate important metrics from the data by using sql commmands on duckdb and visualise the same using graphs. <br>

    a. The other metrics that are currently not available but could be made available are :  

    i. Genre: At the moment there is only track name and artist name available if genre is also available , this would help in targeted campaigns<br>

    ii. rating : It is currently populated as blank in the given data set. If this is populated , then it would help in promoting highly rated songs by all users.<br>

    iii. Geographic location: This is most important column that needs to be available in order understand preferences of users based on their geographic location and optimising campaigns. <br>

    b. The other metrics that I have choosen are 

    i.Total_TracksPlayed<br>
    ii.Unique_Tracks_Played<br>
    iii.Total_ListeningTime_InHours<br>
    iv.Avg_listening_freq_InMins_byMonth<br>
    v.Avg_hour_of_activity<br>
    vi.Avg_Min_of_activity<br>

    The reason for choosing these metrics as they provide an overview of user activity on the platform. The unique tracks gives insight into diversity of tracks played by users.Average hour and min tells using during which time of the day users generally use the platform. Avg listening freq can be used to understand peaks during certain months especially during winter months and better tailor content

<br>
Outputs : <br>

Task1 : Data Ingestion<br>

Data is loaded onto duckdb database by placing dataset.txt file under SourceFiles Folder. There is no need to place the file again to run the solution as ListenBrainz.db(duckdb database) is also attached to the solution and can be accessed straight away.

Task 2 : Data Analysis

a. output can be seen in ./DataAnalysis/summary_metrics_tiles.png file. Giving a first look at the data that is loaded. <br>

b. output of data that is transferred to datascience team. <br>


        user_id                     track_name  ...               Listened_At listening_from
0       cimualte           ドビュッシー/ハイフェッツ:美しい夕暮れ  ... 2019-02-09 06:49:05+00:00           None
1       cimualte        Canon in D Major, P. 37  ... 2019-02-09 06:43:57+00:00           None
2       cimualte  Gaspard de la Nuit: I. Ondine  ... 2019-02-09 06:37:18+00:00           None
3       cimualte                        ジュ・トゥ・ヴ  ... 2019-02-09 06:32:18+00:00           None
4       cimualte                            愛の夢  ... 2019-02-09 06:26:39+00:00           None
...          ...                            ...  ...                       ...            ..


c. below is the output of task 2 questions asked by CEO.<br>

1.Top 10 most active users <br>
┌────────────────┬──────────────┐
│   user_name    │ count_star() │
│    varchar     │    int64     │
├────────────────┼──────────────┤
│ hds            │        46885 │
│ Groschi        │        14959 │
│ Silent Singer  │        13005 │
│ phdnk          │        12861 │
│ 6d6f7274686f6e │        11544 │
│ reverbel       │         8398 │
│ Cl�psHydra     │         8318 │
│ InvincibleAsia │         7804 │
│ cimualte       │         7356 │
│ inhji          │         6349 │
├────────────────┴──────────────┤
│ 10 rows             2 columns │
└───────────────────────────────┘

2.How many users were active on the 1st of March 2019?<br>
┌───────────────────────────┐
│ count(DISTINCT user_name) │
│           int64           │
├───────────────────────────┤
│                        75 │
└───────────────────────────┘

3.For every user, what was the first song they listened to?<br>
┌─────────────────┬──────────────────────────────────────┐
│    user_name    │        First_Song_Listened_To        │
│     varchar     │               varchar                │
├─────────────────┼──────────────────────────────────────┤
│ 6d6f7274686f6e  │ The Leper Affinity                   │
│ Adsky_traktor   │ Сердце с долгом разлучается          │
│ AllSparks       │ Fever                                │
│ AlwinHummels    │ Geef me je angst                     │
│ Arcor           │ Exsultate Justi                      │
│ AscendedGravity │ Amoeba                               │
│ Bezvezenator    │ Devour                               │
│ BiamBioum       │ Beirut (14.12.16 - Live in Paris)    │
│ BlackGauna      │ Visionz                              │
│ Boris_Neo       │ Keep You Close                       │
│    ·            │       ·                              │
│    ·            │       ·                              │
│    ·            │       ·                              │
│ verfyjd         │ The Magic Number                     │
│ vitalikdobriy   │ Я из станицы                         │
│ welldread       │ Arizona                              │
│ whatisbetter    │ Need You Now                         │
│ xokoyotzin      │ First Time                           │
│ xvicarious      │ Beer Bucket List                     │
│ yellams         │ Hold You Under (feat. Marcus Bridge) │
│ zebedeemcdougal │ Roygbiv                              │
│ zergut          │ Chasing The Sun                      │
│ †AtzzkiySotona† │ Meridian                             │
├─────────────────┴──────────────────────────────────────┤
│ 202 rows (20 shown)                          2 columns │
└────────────────────────────────────────────────────────┘


Task 3 : Management Report <br>

output can be seen from the file ./DataAnalysis/Management_Report.png

