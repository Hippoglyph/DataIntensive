import org.apache.spark.streaming.twitter._
import org.apache.spark._
import org.apache.spark.streaming._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import Constants._
//import org.apache.spark.streaming.StreamingContext._


object Trainer {
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

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val conf = new SparkConf().setAppName("BFFs sick stream").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc, Seconds(10))
		ssc.checkpoint("checkpoint")

		val word2vec = new Word2vec(conf)

		val stream = getStream(ssc)

		val results = stream.foreachRDD(rdd => {
			val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
			import spark.implicits._

			val myTweets = rdd.filter(tweet => tweet.getLang() != null && tweet.getLang() == "en")
			if (!myTweets.isEmpty)
				myTweets.map(tweet => {
					val text  = {
						if (tweet.getRetweetedStatus() != null)
							tweet.getRetweetedStatus().getText()
						else
							tweet.getText()
					}
					text
				}).map(word2vec.process).collect()
				println("new tweets: " + myTweets.count)
		})

		ssc.start()
    	ssc.awaitTermination()
	}
}