import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._

def setNotNull(df: DataFrame, columns: Seq[String]) : DataFrame = {
  val schema = df.schema
  // Modify [[StructField] for the specified columns.
  val newSchema = StructType(schema.map {
    case StructField(c, t, _, m) if columns.contains(c) => StructField(c, t, nullable = false, m)
    case y: StructField => y
  })
  // Apply new schema to the DataFrame
  df.sqlContext.createDataFrame(df.rdd, newSchema)
}

val sfmta_raw = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("sfmtaAVLRawData01012013.csv")

val sftmta_time = sfmta_raw.withColumn("REPORT_TIME", to_timestamp($"REPORT_TIME", "MM/dd/yyyy HH:mm:ss"))

val sftmta_prep = setNotNull(sftmta_time, Seq("REPORT_TIME", "VEHICLE_TAG"))

val kuduContext = new KuduContext("localhost:7051,localhost:7151,localhost:7251", spark.sparkContext)

// Delete the table if it already exists.
if(kuduContext.tableExists("sfmta_kudu")) {
	kuduContext.deleteTable("sfmta_kudu")
}

kuduContext.createTable("sfmta_kudu", sftmta_prep.schema,
  /* primary key */ Seq("REPORT_TIME", "VEHICLE_TAG"),
  new CreateTableOptions().setNumReplicas(3).addHashPartitions(List("VEHICLE_TAG").asJava, 4))

kuduContext.insertRows(sftmta_prep, "sfmta_kudu")

val sfmta_kudu = spark.read.option("kudu.master", "localhost:7051,localhost:7151,localhost:7251").option("kudu.table", "sfmta_kudu").option("kudu.scanLocality", "leader_only").format("kudu").load

sfmta_kudu.createOrReplaceTempView("sfmta_kudu")

spark.sql("SELECT count(*) FROM sfmta_kudu").show()
