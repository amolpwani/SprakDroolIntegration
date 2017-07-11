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


object KafkaTSDBUpdater {
	val cm = new PoolingClientConnectionManager()
			cm.setMaxTotal(100);
	val client = new DefaultHttpClient(cm)

			// val client = HttpClientBuilder.create.build
}

/**
 * A Simple time-series client without any optimizations :)
 */
class KafkaTSDBUpdater (url : String) extends Serializable {

	def loadPatientStats (patient : Patient) {

		println("===============================START================================================")
		val metricList = new ArrayList[OpenTSDBMessageElement]()
		val jmap = new MetricsTags(patient.getLocation)

		val evalTimestamp = patient.getEvaluationDate

		println("Jmap "+ jmap)
		println("evalTimestamp "+ evalTimestamp)
		println("sirs "+ patient.getSirsFlag)




		val sirsMetric = new OpenTSDBMessageElement("sirs", evalTimestamp, patient.getSirsFlag, jmap)
		metricList.add(sirsMetric)

		val sepsisMetric = new OpenTSDBMessageElement("sepsis", evalTimestamp, patient.getSepsisFlag, jmap)
		metricList.add(sepsisMetric)

		val severeSepsisMetric = new OpenTSDBMessageElement("severeSepsis", evalTimestamp, patient.getSevereSepsisFlag, jmap)
		metricList.add(severeSepsisMetric)

		val septicShockMetric = new OpenTSDBMessageElement("septicShock", evalTimestamp, patient.getSepticShockFlag, jmap)
		metricList.add(septicShockMetric)

		val organMetric = new OpenTSDBMessageElement("organDysfunctionSyndrome", evalTimestamp, patient.getOrganDysfunctionSyndrome, jmap)
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
