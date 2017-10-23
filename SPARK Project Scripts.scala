// Loading Song artist file 
case class SongArt (songid: String , artistid: String)
val track = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/song-artist.txt").map(_.split(",")).map(p => SongArt(p(0), p(1))).toDF()
track.registerTempTable("songArt");
val songtrack = sqlContext.sql("SELECT * FROM songArt")

//Loading Station and Geoid file
case class Station (stationid: String , geoid: String)
val track = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/stn-geocd.txt").map(_.split(",")).map(p => Station(p(0), p(1))).toDF()
track.registerTempTable("station");
val Stngeo = sqlContext.sql("SELECT * FROM station")

// Loading Subscription start time and load time
case class Subscription(user_id: String , subs_start_date: String, subs_end_date: String)
val track = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/user-subscn.txt").map(_.split(",")).map(p => Subscription(p(0), p(1),p(2))).toDF()
track.registerTempTable("usersub");
val subs = sqlContext.sql(" SELECT * FROM usersub ")

// Loading Music play file
case class musicplay(User_id: String , song_id: String, artistid: String, start_ts: Long, end_ts: Long , substart_ts: Long, geocd: String, station_id: String , songtype: Long, dislike: Long, like: Long)
val df = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/file.txt").map(_.split(",")).map(p => musicplay(p(0) , p(1) , p(2) , p(3).toLong , p(4).toLong , p(5).toLong , p(6) , p(7) , p(8).toLong , p(9) .toLong, p(10) .toLong)).toDF()

df.registerTempTable("musicrecords");
val main = sqlContext.sql("SELECT * FROM musicrecords")

//  Loading Subscription start time and load time
case class Subscription(user_id: String , subs_start_date: String, subs_end_date: String)
val track = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/user-subscn.txt").map(_.split(",")).map(p => Subscription(p(0), p(1),p(2))).toDF()
track.registerTempTable("usersub");
val subs = sqlContext.sql(" SELECT * FROM usersub ")

// Apply data enrichment process
val dataench = sqlContext.sql("SELECT count(*) FROM musicrecords ")

// If both like and dislike are 1, consider that record to be invalid.
val invaliddata = sqlContext.sql("SELECT Distinct User_id FROM musicrecords where like ==1 and dislike ==1 ")
invaliddata.registerTempTable("invalidrec");
val indrec = sqlContext.sql(" SELECT * FROM invalidrec ")


// Filter valid record based on data enrichment rules
val dataench= sqlContext.sql("SELECT User_id,start_ts,end_ts,substart_ts,songtype,case when (M.geocd is null or M.geocd ='') then S.geoid else M.geocd end geocd, M.station_id, M.song_id, case when (M.artistid is null or M.artistid='') then A.artistid else M.artistid end artistid,case when like='' or like is null then 0 else like end like, case when dislike='' or dislike is null then 0 else dislike end dislike,case when songtype ==0 then 'completed_successfully' when songtype ==1 then 'skipped' when songtype == 2 then 'paused' when songtype == 3 then 'device issue' end song_type ,case when like ==1 and dislike ==1 then 0 else 1 end flag FROM musicrecords M left join station S on S.stationid = M.station_id left join songArt A on A.songid =M.song_id where ((M.User_id!='' or M.User_id is null) and  M.station_id !='' )") 

// Register valid records
dataench.registerTempTable("filtermusic");
val validrec = sqlContext.sql(" SELECT * FROM filtermusic where flag ==1 ")

// Save valid /invalid record at HDFS project folder

// Valid records

validrec.write.format("orc").save("hdfs://quickstart.cloudera:8020/user/cloudera/Project/validrec.orc")

// Invalid records

invaliddata.write.format("orc").save("hdfs://quickstart.cloudera:8020/user/cloudera/Project/invalidrec.orc")


//Data Analysis 1.0
//Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.

val da1 = sqlContext.sql (" SELECT station_id, count(distinct song_id) as cnt_song_id,min(song_id) as songid, min(User_id)as Userid,count(distinct (User_id)) as cnt_user_id FROM filtermusic where flag ==1 and like == 1 group by station_id ").limit(10) 

da1.show

//Data Analysis 2.0

//Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or 'unsubscribed'. An unsubscribed user is the one whose record is either not present in Subscribed_users lookup table or has subscription_end_date earlier than the timestamp of the song played by him.

val da2 = sqlContext.sql (" SELECT M.User_id,M.start_ts,M.end_ts,M.substart_ts,U.user_id,subs_start_date,subs_end_date,case when U.user_id is null or (M.substart_ts > U.subs_end_date) then 'Unsubscribe' when U.user_id is not null and (M.substart_ts <=  U.subs_end_date) then 'Subscribe' end SubcriptionType,ABS(M.end_ts - M.start_ts)as Duration  FROM filtermusic M LEFT JOIN usersub U on u.user_id= M.User_id where M.flag ==1 ")

da2.registerTempTable("da2");

val da21 = sqlContext.sql(" SELECT sum(Duration) as TotalDuration, SubcriptionType  FROM da2 group by SubcriptionType ")
da21.show


case class userartist(user_id: String , artistid: String)

val track = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/artistoutput").map(_.split(",")).map(p => userartist(p(0), p(1))).toDF()

track.registerTempTable("userArtist");
val userartist = sqlContext.sql(" SELECT * FROM userArtist ")

//Data Analayis 3
val da3 = sqlContext.sql("SELECT count(distinct A.user_id) count_userid, A.artistid, song_id, min(A.user_id) userid FROM userArtist A INNER JOIN filtermusic M on A.user_id = M.User_id AND A.artistid= M.artistid and flag=1 group by A.artistid,song_id ").limit(10)
da3.show

//Data Analayis 4
val da4 = sqlContext.sql("SELECT song_id, ABS(end_ts - start_ts) AS duration,like,songtype FROM filtermusic WHERE flag==1 and (like=1 OR songtype=0) ORDER BY duration DESC ")
da4.registerTempTable("da4");

val dataanalyis4 = sqlContext.sql(" SELECT song_id , sum(duration) as Duration FROM da4 group by song_id order by duration DESC ").limit(10).show
dataanalyis4.show

//Data Analayis 5
val da5 = sqlContext.sql (" SELECT M.User_id,M.start_ts,M.end_ts,M.substart_ts,subs_start_date,subs_end_date, case when U.user_id is null or (M.substart_ts > U.subs_end_date) then 'Unsubscribe' when U.user_id is not null and (M.substart_ts <=  U.subs_end_date) then 'Subscribe' end SubcriptionType, ABS(M.end_ts - M.start_ts) as Duration  FROM filtermusic M LEFT JOIN usersub U on u.user_id= M.User_id where M.flag ==1 ")

da5.registerTempTable("da5");
val da51 = sqlContext.sql(" SELECT User_id,Duration, SubcriptionType  FROM da5 where  SubcriptionType ='Unsubscribe' order by Duration desc").limit(10)

da51.show

//Create an output folder and store all analysis results in HDFS

da1.rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da1")
da21.rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da2")

da3.rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da3")
dataanalyis4.rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da4")
da51.rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da5")

// Export data to mysql for visualization or further analysis

sqoop export --connect jdbc:mysql://localhost/project --username root --password cloudera --table da1  --export-dir hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da1/part-00000
sqoop export --connect jdbc:mysql://localhost/project --username root --password cloudera --table da2  --export-dir hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da211/part-00193
sqoop export --connect jdbc:mysql://localhost/project --username root --password cloudera --table da3  --export-dir hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da3/part-00000
sqoop export --connect jdbc:mysql://localhost/project --username root --password cloudera --table da4  --export-dir hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da4/part-00000
sqoop export --connect jdbc:mysql://localhost/project --username root --password cloudera --table da5  --export-dir hdfs://quickstart.cloudera:8020/user/cloudera/Project/Result/da5/part-00000

