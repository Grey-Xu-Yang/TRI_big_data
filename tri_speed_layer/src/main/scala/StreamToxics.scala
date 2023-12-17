import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamToxics {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("greyxu_latest_tri"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: StreamToxicData <brokers>
                            |  <brokers> is a list of one or more Kafka brokers
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamToxicData")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Define Kafka topic
    val topicsSet = Set("greyxu_tri")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "toxic_data_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Deserialize and process records
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[ToxicReport]))

    // Write to HBase table
    val batchStats = reports.map(tr => {
      // Append a timestamp to the sectorCode to create a unique row key
      val uniqueRowKey = Bytes.toBytes(tr.sectorCode + "_" + System.currentTimeMillis())
      val put = new Put(uniqueRowKey)
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("on_site_release_total"), Bytes.toBytes(tr.on_site_release_total))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("off_site_release_total"), Bytes.toBytes(tr.off_site_release_total))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("energy_recover_on"), Bytes.toBytes(tr.energy_recover_on))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("energy_recover_of"), Bytes.toBytes(tr.energy_recover_of))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("recycling_on_site"), Bytes.toBytes(tr.recycling_on_site))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("recycling_off_site"), Bytes.toBytes(tr.recycling_off_site))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("treatment_on_site"), Bytes.toBytes(tr.treatment_on_site))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("treatment_off_site"), Bytes.toBytes(tr.treatment_off_site))
      put.addColumn(Bytes.toBytes("sector"), Bytes.toBytes("production_waste"), Bytes.toBytes(tr.production_waste))

      table.put(put)
    })
    batchStats.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

