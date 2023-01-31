# Spark Cluster with Docker & docker-compose(2021 ver.)

# General

A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

The Docker compose will create the following containers:

container|Exposed ports
---|---
spark-master|9090 7077
spark-worker-1|9091
spark-worker-2|9092
demo-database|5432

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the image


```sh
docker build -t cluster-apache-spark:3.0.2 .
```

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker-compose up -d
```

## Validate your cluster

Just validate your cluster accesing the spark UI on each worker & master URL.

### Spark Master

http://localhost:9090/

![alt text](docs/spark-master.png "Spark master UI")

### Spark Worker 1

http://localhost:9091/

![alt text](docs/spark-worker-1.png "Spark worker 1 UI")

### Spark Worker 2

http://localhost:9092/

![alt text](docs/spark-worker-2.png "Spark worker 2 UI")


# Resource Allocation 

This cluster is shipped with three workers and one spark master, each of these has a particular set of resource allocation(basically RAM & cpu cores allocation).

* The default CPU cores allocation for each spark worker is 1 core.

* The default RAM for each spark-worker is 1024 MB.

* The default RAM allocation for spark executors is 256mb.

* The default RAM allocation for spark driver is 128mb

* If you wish to modify this allocations just edit the env/spark-worker.sh file.

# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
data|/opt/spark-data| Used to make available your app's data on all workers & master

This is basically a dummy DFS created from docker Volumes...(maybe not...)

# Run Sample applications


## NY Bus Stops Data [Pyspark]

This programs just loads archived data from [MTA Bus Time](http://web.mta.info/developers/MTA-Bus-Time-historical-data.html) and apply basic filters using spark sql, the result are persisted into a postgresql table.

The loaded table will contain the following structure:

latitude|longitude|time_received|vehicle_id|distance_along_trip|inferred_direction_id|inferred_phase|inferred_route_id|inferred_trip_id|next_scheduled_stop_distance|next_scheduled_stop_id|report_hour|report_date
---|---|---|---|---|---|---|---|---|---|---|---|---
40.668602|-73.986697|2014-08-01 04:00:01|469|4135.34710710144|1|IN_PROGRESS|MTA NYCT_B63|MTA NYCT_JG_C4-Weekday-141500_B63_123|2.63183804205619|MTA_305423|2014-08-01 04:00:00|2014-08-01

To submit the app connect to one of the workers or the master and execute:

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--jars /opt/spark-apps/postgresql-42.2.22.jar \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/main.py
```

![alt text](./articles/images/pyspark-demo.png "Spark UI with pyspark program running")

## MTA Bus Analytics[Scala]

This program takes the archived data from [MTA Bus Time](http://web.mta.info/developers/MTA-Bus-Time-historical-data.html) and make some aggregations on it, the calculated results are persisted on postgresql tables.

Each persisted table correspond to a particullar aggregation:

Table|Aggregation
---|---
day_summary|A summary of vehicles reporting, stops visited, average speed and distance traveled(all vehicles)
speed_excesses|Speed excesses calculated in a 5 minute window
average_speed|Average speed by vehicle
distance_traveled|Total Distance traveled by vehicle


To submit the app connect to one of the workers or the master and execute:

```sh
/opt/spark/bin/spark-submit --deploy-mode cluster \
--master spark://spark-master:7077 \
--total-executor-cores 1 \
--class mta.processing.MTAStatisticsApp \
--driver-memory 1G \
--executor-memory 1G \
--jars /opt/spark-apps/postgresql-42.2.22.jar \
--conf spark.driver.extraJavaOptions='-Dconfig-path=/opt/spark-apps/mta.conf' \
--conf spark.executor.extraJavaOptions='-Dconfig-path=/opt/spark-apps/mta.conf' \
/opt/spark-apps/mta-processing.jar
```

You will notice on the spark-ui a driver program and executor program running(In scala we can use deploy-mode cluster)

![alt text](./articles/images/stats-app.png "Spark UI with scala program running")


# Summary

* We compiled the necessary docker image to run spark master and worker containers.

* We created a spark standalone cluster using 2 worker nodes and 1 master node using docker && docker-compose.

* Copied the resources necessary to run demo applications.

* We ran a distributed application at home(just need enough cpu cores and RAM to do so).

# Why a standalone cluster?

* This is intended to be used for test purposes, basically a way of running distributed spark apps on your laptop or desktop.

* This will be useful to use CI/CD pipelines for your spark apps(A really difficult and hot topic)

# Steps to connect and use a pyspark shell interactively

* Follow the steps to run the docker-compose file. You can scale this down if needed to 1 worker. 

```sh
docker-compose up --scale spark-worker=1
docker exec -it docker-spark-cluster_spark-worker_1 bash
apt update
apt install python3-pip
pip3 install pyspark
pyspark
```

# What's left to do?

* Right now to run applications in deploy-mode cluster is necessary to specify arbitrary driver port.

* The spark submit entry in the start-spark.sh is unimplemented, the submit used in the demos can be triggered from any worker

# Note
trong watermark, thì cột dùng làm watermark phải dùng group by trên nó
dữ liệu log gửi qua kafka thì encode lại theo utf-8 rồi decode ra, dữ liệu đầu vào nếu nhiều thì dùng json.dumps sau đó json.loads ra lại, còn không thì không cần dumps vì nó sẽ chuyển các ký tự thành escape character rất khó xử lý   


Trong thư mục apps:

-gen_date.py: sinh ra các điểm thời gian ngẫu nhiên

-main_test.py: test cách đọc dữ liệu trong spark từ file 

-main_bigquery: đọc/ghi dữ liệu bằng spark với bigquery

-parse_log_streaming: parse log theo thời gian thực bằng cách đọc dữ liệu từ kafka

-parse_log.py: parse 1 file log một lần duy nhất

-spark_streaming_kafka.py: test kafka, spark structured streaming


Trong thư mục kafka_code, chứa các code của kafka:

-create_topic.sh: đầu vào là xâu "demo" để tạo topic demo cũng như install package kafka-python

-consume_data_from_kafka.py: để consume data ở topic demo

-feed_data_to_kafka.py: đẩy dữ liệu giống nhau vào kafka 

-feed_log_to_kafka.py: đẩy dữ liệu log từ access_log.log

-gen_and_push_data.py: sinh dữ liệu random rồi đẩy trực tiếp vào kafka

-gen_data.py: sinh dữ liệu rồi lưu xuống file access_log.log thư mực kafka_code/, dữ liệu này lấy chuỗi thời gian từ file listtimestamp.csv
