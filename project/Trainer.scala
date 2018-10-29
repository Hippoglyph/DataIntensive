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

//import org.apache.spark.mllib.linalg._
import breeze.linalg._

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
	    session.execute("CREATE TABLE IF NOT EXISTS project.w (id int PRIMARY KEY, vector list<double>);")
	    session
	}

	def writeToCassandra(wordData: scala.collection.mutable.Map[String, (Int, DenseVector[Double])], model: DenseMatrix[Double],sc: SparkContext){
		val mapRDD = sc.parallelize((wordData.map{case (k, (id, vec)) => (k, id, vec.toArray.toSeq)}).toSeq)
		var rowCounter = -1
		val modelRDD = sc.parallelize((model(*,::).map(x => {
			rowCounter += 1
			(rowCounter, x.toArray.toSeq)
		})).toArray.toSeq)
		mapRDD.saveToCassandra("project", "words", SomeColumns("word", "id", "vector"))
		modelRDD.saveToCassandra("project", "w", SomeColumns("id", "vector"))
	}

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val conf = new SparkConf().setAppName("BFFs sick stream").setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc, Seconds(10))
		ssc.checkpoint("checkpoint")
		val session = createCassandra(conf)

		var wordData = Word2vec.getInitWordData(conf)
		var contenderWords = scala.collection.mutable.Map[String, Int]()
		var model = Word2vec.getInitModel(conf)

		val stream = getStream(ssc)

		val results = stream.foreachRDD(rdd => {
			val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
			import spark.implicits._

			val wordDataBC = rdd.sparkContext.broadcast(wordData)
			val modelBC = rdd.sparkContext.broadcast(model)
			//val contenderWordsBC = rdd.sparkContext.broadcast(contenderWords)

			val myTweets = rdd.filter(tweet => tweet.getLang() != null && tweet.getLang() == "en")
			val processed = myTweets.map(tweet => {
					if (tweet.getRetweetedStatus() != null)
						tweet.getRetweetedStatus().getText()
					else
						tweet.getText()
				}).map(tweet => Word2vec.process(tweet, wordDataBC.value, modelBC.value))
			//Update model
			val rddWGrad = processed.map(x=>x._2)
			val wGradient = rddWGrad.reduce((a,b) => a + b) :*= (1.0/rddWGrad.count)
			model = model - (wGradient :*= Constants.learningRate())
			
			val rddXGrad = processed.flatMap(x=>x._3)
			val xGradient = rddXGrad.groupByKey().map{x=> 
				val count = (x._2).size
				val sum = x._2.reduce(_+_)
				(x._1, sum :*= (1.0/count))
			}

			xGradient.collect.foreach{grad=>
				wordData(grad._1) = (wordData(grad._1)._1, wordData(grad._1)._2 - (grad._2 :*= Constants.learningRate()))
			} 
			
			val uniqueWords = processed.flatMap(x => x._1).distinct().collect()
			if (uniqueWords.size > 0){
				Word2vec.addToContender(contenderWords, uniqueWords)
				val cancer = Word2vec.addToWordData(wordData, contenderWords, model)
				if(cancer.rows > 0)
					model = DenseMatrix.vertcat(model,cancer)
			}

			println("Total words: " + wordData.keys.size)
			println("Contenders: " + contenderWords.keys.size)
			println("new tweets: " + myTweets.count)
		})

		ssc.start()
		var loopCounter = 0
		while(true){
			Thread.sleep(10000)
			loopCounter += 1
			if(loopCounter % (6 * 5) == 0)
				writeToCassandra(wordData,model,sc)
			if(loopCounter % (6 * 10) ==  0)
				Word2vec.cleanUpContender(contenderWords)

			loopCounter = loopCounter % (6 * 30)
		}
		
		ssc.awaitTermination()
    	session.close()
	}
}