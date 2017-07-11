package com.cloudera.sprue

import org.apache.spark.streaming.kafka._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.kie.api.runtime.StatelessKieSession
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.hadoop.hbase.HBaseConfiguration
import com.cloudera.spark.hbase.HBaseContext

object KafkaStream {

	def main(args: Array[String]) {
		if (args.length < 3) {
			println("Usage: SepsisStream <xlsFileName> <zk-quorum-for-hbase> <tsdburl>")
			System.exit(1)
		}

		//params
		val xlsFileName = args(0)
				val zkString = args(1)
				val url = args(2)

				//5 seconds interval
				val batchInterval = 5

				//check if the rules file exists
				if (!Files.exists(Paths.get(xlsFileName))) {
					println("Error: Rules file [" + xlsFileName + "] is missing")
					return
				}

		//setup spark. Toggle below two lines for local v/s yarn
		val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Sepsis Rules Streaming Evaluation")

				val ssc = new StreamingContext(sparkConf, Seconds(1))
				val kafkaTopic = "Hello-Kafka"
				val kafkaStream = KafkaUtils.createStream(ssc, zkString,"spark-consumer", Map(kafkaTopic -> 5))


				//setup rules executor
				val rulesExecutor = new RulesExecutor(xlsFileName)

				//setup tsdbupdater
				val tsdbUpdater = new KafkaTSDBUpdater(url)

				//to-add filler logic to create snapshot

				//logic for each dstream
				kafkaStream.foreachRDD(rdd => {


					println("--- New RDD with " + rdd.partitioner.size + "partitions and " + rdd.count() + " records")
					for(item <- rdd.collect().toArray) {
						//rulesExecutor.evalRules(item)
						println("---------------------------$$$$$$$$$$$$$$---------------" + item)
						//tsdbUpdater.loadPatientStats(item)
					}
				})

				ssc.start()
				ssc.awaitTermination()
	}
}