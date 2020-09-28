spark-shell --packages org.apache.kudu:kudu-spark2_2.11:1.12.0  --master local[*] --conf spark.driver.bindAddress=127.0.0.1 --conf spark.driver.host=127.0.0.1 --conf spark.driver.port=7777

>:load load_data.scala

docker run -d --name kudu-impala --network="docker_default" \
  -p 21000:21000 -p 21050:21050 -p 25000:25000 -p 25010:25010 -p 25020:25020 \
  --memory=4096m apache/kudu:impala-latest impala

docker exec -it kudu-impala impala-shell

