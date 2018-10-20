import org.apache.spark.streaming.twitter._
import org.apache.spark._
import org.apache.spark.streaming._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import Constants._
import Word2vec._
//import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


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

	def createCassandra(conf: SparkConf) = {
		val sparkn = SparkSession.builder.config(conf).config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
		val sc = sparkn.sparkContext
		import sparkn.implicits._

		val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
	    val session = cluster.connect()
	    session.execute("CREATE KEYSPACE IF NOT EXISTS project WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	    session.execute("CREATE TABLE IF NOT EXISTS project.words (word text PRIMARY KEY, id int, vector list<double>);")
	    session
	}

	def writeToCassandra(wordData: scala.collection.mutable.Map[String, Int], sc: SparkContext){
		val mapRDD = sc.parallelize((wordData.map{case (k, v) => (k, v, List(2, 2))}).toSeq)
		mapRDD.saveToCassandra("project", "words", SomeColumns("word", "id", "vector"))
	}

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val conf = new SparkConf().setAppName("BFFs sick stream").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc, Seconds(10))
		ssc.checkpoint("checkpoint")
		val session = createCassandra(conf)

		val stream = getStream(ssc)

		var wordData = Word2vec.getInitWordData()
		var contenderWords = Word2vec.getInitWordData()

		val results = stream.foreachRDD(rdd => {
			val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
			import spark.implicits._

			val myTweets = rdd.filter(tweet => tweet.getLang() != null && tweet.getLang() == "en")
			val processed = myTweets.map(tweet => {
					if (tweet.getRetweetedStatus() != null)
						tweet.getRetweetedStatus().getText()
					else
						tweet.getText()
				}).map(tweet => Word2vec.process(tweet, wordData))
			val uniqueWords = processed.flatMap(x => x).distinct().collect()
			if (uniqueWords.size > 0){
				Word2vec.addToContender(contenderWords, uniqueWords)
				Word2vec.addToWordData(wordData, contenderWords)
			}

			
			println("Total words: " + wordData.keys.size)
			println("Contenders: " + contenderWords.keys.size)
			println("new tweets: " + myTweets.count)
		})

		ssc.start()
		while(true){
			Thread.sleep(10000)
			writeToCassandra(wordData,sc)
		}
		
		ssc.awaitTermination()
    	session.close()
	}
}