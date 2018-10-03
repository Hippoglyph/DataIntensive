package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setAppName("BFFs sick stream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoint")
    val kafkaConf = Map("metadata.broker.list" -> "localhost:9092", "zookeeper.connect" -> "localhost:2181", "group.id" -> "kafka-spark-streaming", "zookeeper.connection.timeout.ms" -> "1000")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))

    val pairs = messages.map(x => x._2.split(",")).map(x => (x(0), x(1).toDouble))

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
		val (cumSum, n) = state.getOption.getOrElse((0.0,0))
		val newSum = cumSum + value.get
		val newN = n + 1
		state.update((newSum,newN))
		(key, newSum/newN)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("wordcount_keyspace", "words", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}