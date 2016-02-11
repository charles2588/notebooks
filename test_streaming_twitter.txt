
%Addjar -f https://github.com/ibm-cds-labs/spark.samples/raw/master/dist/streaming-twitter-assembly-1.2.jar

val demo = com.ibm.cds.spark.samples.StreamingTwitter //Shorter handle 
    //Twitter OAuth params from section above
    demo.setConfig("twitter4j.oauth.consumerKey”,”xxxxx”)
    demo.setConfig("twitter4j.oauth.consumerSecret”,”xxxxxx”)
    demo.setConfig("twitter4j.oauth.accessToken”,”xxxxxx”)
    demo.setConfig("twitter4j.oauth.accessTokenSecret”,”xxxxxx”)
    //Tone Analyzer service credential copied from section above
    demo.setConfig("watson.tone.url","https://gateway.watsonplatform.net/tone-analyzer-experimental/api")
    demo.setConfig("watson.tone.password”,”xxxx”)
    demo.setConfig("watson.tone.username”,”xxxx”)

import org.apache.spark.streaming._

demo.startTwitterStreaming(sc, Seconds(60))

val (sqlContext, df) = demo.createTwitterDataFrames(sc)

val fullSet = sqlContext.sql("select text from tweets")

fullSet.show

fullSet.repartition(1).saveAsParquetFile("swift://notebooks.spark/tweetsFull.parquet")

val angerSet = sqlContext.sql("select author, text, Anger from tweets where Anger > 60")
println(angerSet.count)
angerSet.show


