package com.cloudera.sprue

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import java.util.ArrayList
import org.apache.http.client.entity.UrlEncodedFormEntity
import com.google.gson.Gson
import java.util.HashMap
import java.lang.reflect.Type
import com.google.gson.reflect.TypeToken
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.sql.Row
import org.apache.http.util.EntityUtils
import org.apache.http.impl.conn.PoolingClientConnectionManager


case class MetricsTags(state: String)
case class OpenTSDBMessageElement(metric: String, timestamp: Long, value: Long, tags: MetricsTags)

object TSDBUpdater {
	val cm = new PoolingClientConnectionManager()
			cm.setMaxTotal(100);
	val client = new DefaultHttpClient(cm)

			// val client = HttpClientBuilder.create.build
}

/**
 * A Simple time-series client without any optimizations :)
 */
class TSDBUpdater (url : String) extends Serializable {

	def loadPatientStats (row : Row) {

		println("===============================START================================================")
		val metricList = new ArrayList[OpenTSDBMessageElement]()
		val jmap = new MetricsTags(row.getString(1))

		val evalTimestamp = row.getLong(2)

		println("Jmap "+row.getString(1))
		println("evalTimestamp "+row.getLong(2))
		println("sirs "+row.getLong(3))




		val sirsMetric = new OpenTSDBMessageElement("sirs", evalTimestamp, row.getLong(3), jmap)
		metricList.add(sirsMetric)

		val sepsisMetric = new OpenTSDBMessageElement("sepsis", evalTimestamp, row.getLong(4), jmap)
		metricList.add(sepsisMetric)

		val severeSepsisMetric = new OpenTSDBMessageElement("severeSepsis", evalTimestamp, row.getLong(5), jmap)
		metricList.add(severeSepsisMetric)

		val septicShockMetric = new OpenTSDBMessageElement("septicShock", evalTimestamp, row.getLong(6), jmap)
		metricList.add(septicShockMetric)

		val organMetric = new OpenTSDBMessageElement("organDysfunctionSyndrome", evalTimestamp, row.getLong(7), jmap)
		metricList.add(organMetric)

		val metricsAsJson = new Gson().toJson(metricList)

		println("metricsAsJson ::" + metricsAsJson)
		val post = new HttpPost(url)

		post.setHeader("Content-type", "application/json");
		post.setEntity(new StringEntity(metricsAsJson));
		synchronized {
			val response = TSDBUpdater.client.execute(post)
					val entity = response.getEntity();
			EntityUtils.consumeQuietly(entity)
		}

		println("===============================END================================================")
		//  println("response =====" + response.toString())
	}
}
