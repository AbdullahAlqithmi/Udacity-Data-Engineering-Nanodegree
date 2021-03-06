<h1>Data Lake Project</h1>

<h3>Project Background</h3>
<pA music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.</p>

<p>As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.</p>

<h3>Project Description</h3>

<p>In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.</p>




<h3>Datasets</h3>
<h5>Song Dataset</h5>
<p>The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.</p>

<h5>Log Dataset</h5>
<p>The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.</p>

<h3>Project Used Files</h3>
<ol>
<li>etl.py is where the loading of data from S3 into staging tables on Redshift will occur and then processing that data into your analytics tables on Redshift.</li>
<li>README.md provides discussion on the project.</li>
<li>dl.cfgcontains: AWS credentials</li>
</ol>


<h3>Database Schema </h3>

<p>Using the song and log datasets, I used to create a star schema optimized for queries on song play analysis. This includes the following tables.</p>

<img src="Song_ERD.png" alt="ER" width="5000" height="6000">

<h5>Fact Table</h5>
<ol>
<li>songplays - records in log data associated with song plays i.e. records with page NextSong</li>
<ol><ul>songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</ul></ol></ol>

<h5>Dimension Tables</h5>
<ol>
<li>users - users in the app</li>
<ol><ul>user_id, first_name, last_name, gender, level</ul></ol>
<li>songs - songs in music database</li>
<ol><ul>song_id, title, artist_id, year, duration</ul></ol>
<li>artists - artists in music database</li>
<ol><ul>artist_id, name, location, latitude, longitude</ul></ol>
<li>time - timestamps of records in songplays broken down into specific units</li>
<ol><ul>start_time, hour, day, week, month, year, weekday</ul></ol></ol>

<h3>ETL Pipeline</h3>
<p>This file etl.py contains all the ETL Pipeline code for each table. Thus by running this file, user will be able to load the data from Datasets to the tables.</p>