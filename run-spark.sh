spark-shell --packages org.apache.kudu:kudu-spark2_2.11:1.12.0  --master local[*] --conf spark.driver.bindAddress=127.0.0.1 --conf spark.driver.host=127.0.0.1 --conf spark.driver.port=7777

>load: load_data.scala
