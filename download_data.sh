wget http://kudu-sample-data.s3.amazonaws.com/sfmtaAVLRawData01012013.csv.gz

gunzip -c sfmtaAVLRawData01012013.csv.gz | tr -d '\r' | \
  sed 's/PREDICTABLE/PREDICTABLE\n/g' > sfmtaAVLRawData01012013.csv