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

def list_tables(): List[String] = {
  import java.sql.Connection
  import java.sql.DatabaseMetaData;
  import java.sql.DriverManager
  import java.sql.ResultSet
  import java.sql.Statement

  //Registering the Driver
  Class.forName("com.mysql.cj.jdbc.Driver")

  val mysqlUrl = "jdbc:mysql://relational.fit.cvut.cz/financial?serverTimezone=UTC";
  val con = DriverManager.getConnection(mysqlUrl, "guest", "relational")

  System.out.println("Connection established......")

  //Retrieving the meta data object
  val metaData = con.getMetaData()
  
  val types = Array("TABLE");

  //Retrieving the columns in the database
  val tables = metaData.getTables(null, null, "%", types)

  println(tables)

  while (tables.next()) {
      System.out.println(tables.getString("TABLE_NAME"));
  }

  List()
}

def load_to_kudu(source: String, table: String, keys: Seq[String]) = {
  val sfmta_raw = spark.read.format("jdbc").option("url", "jdbc:mysql://relational.fit.cvut.cz/financial?serverTimezone=UTC").option("dbtable", source).option("user", "guest").option("password", "relational").load()

  sfmta_raw.createOrReplaceTempView(table)

  val notNullWhere = keys.map(x => s"$x IS NOT NULL").mkString(" AND ")

  var df = spark.sql(s"SELECT * FROM $table WHERE $notNullWhere")

  df = setNotNull(df, keys)

  val kuduContext = new KuduContext("localhost:7051,localhost:7151,localhost:7251", spark.sparkContext)

  // Delete the table if it already exists.
  if(kuduContext.tableExists(table)) {
    kuduContext.deleteTable(table)
  }

  kuduContext.createTable(table, df.schema, keys, new CreateTableOptions().setNumReplicas(3).addHashPartitions(keys.toList.asJava, 4))

  kuduContext.insertRows(df, table)

  val sfmta_kudu = spark.read.option("kudu.master", "localhost:7051,localhost:7151,localhost:7251").option("kudu.table", table).option("kudu.scanLocality", "leader_only").format("kudu").load

  sfmta_kudu.createOrReplaceTempView(table)

  spark.sql(s"SELECT count(*) FROM $table").show()
}

// load_to_kudu("card", "card", List("card_id", "disp_id"))

// load_to_kudu("loan", "loan", List("loan_id", "account_id"))

// load_to_kudu("trans", "trans", List("trans_id", "account_id"))

// load_to_kudu("disp", "disp", List("disp_id", "client_id"))

// load_to_kudu("account", "account", List("account_id", "district_id"))

// load_to_kudu("client", "client", List("client_id", "district_id"))

// load_to_kudu("district", "district", List("district_id"))

// load_to_kudu("`order`", "orders", List("order_id", "account_id"))