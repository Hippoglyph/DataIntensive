import org.apache.spark.streaming.twitter._
import org.apache.spark._
import org.apache.spark.streaming._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.sql._
import Constants._
//import org.apache.spark.streaming.StreamingContext._


object TweetClassifier {
	def getStream(ssc: StreamingContext) = {
		val builder = new ConfigurationBuilder()
	    builder.setOAuthConsumerKey(Constants.consumerKey())
	    builder.setOAuthConsumerSecret(Constants.consumerSecret())
	    builder.setOAuthAccessToken(Constants.accessToken())
	    builder.setOAuthAccessTokenSecret(Constants.accessSecret())
	    builder.setTweetModeExtended(true)
	    val configuration = builder.build()

		TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(configuration)))
	}

	def main(args: Array[String]) {

		val conf = new SparkConf().setAppName("BFFs sick stream").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(10))
		ssc.checkpoint("checkpoint")

		val stream = getStream(ssc)

		val results = stream.foreachRDD(rdd => {
			val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
			import spark.implicits._

			val myTweets = rdd.filter(tweet => tweet.getPlace() != null).filter(tweet => tweet.getPlace().getCountry() != null).filter(tweet => tweet.getLang() != null && tweet.getLang() == "en")
			if (!myTweets.isEmpty)
				myTweets.map(tweet => {
					val text  = {
						if (tweet.getRetweetedStatus() != null)
							tweet.getRetweetedStatus().getText()
						else
							tweet.getText()
					}
					(tweet.getPlace().getCountry, text)
				}).toDF("Country", "Tweet").coalesce(1).write.partitionBy("Country").mode(SaveMode.Append).json("tweets")
		})

		ssc.start()
    	ssc.awaitTermination()
	}
}