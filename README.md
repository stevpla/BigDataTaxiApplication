# BigDataTaxiApplication Project

A Java software project based on Big Data platforms. The concept is been based on Hadoop and Spark systems. And taxi routes (.csv) file is stored into hdfs and upon this, queries are executed for different scenarios. For this project there are specific prerequisites and steps in order to run the software and the queries.
For the docker spark cluster, all the docker files and scripts i used, belongs to the **rubenafo's** github user. For more info about *Docker-Spark-Cluster*, please refer to **rubenafo's** github repository  [rubenafo/docker-spark-cluster](https://github.com/rubenafo/docker-spark-cluster).


## Prerequisites

Tools and software you need to deploy project successfully. 
* **Operating System**
The project has been build upon CentOS 6 OS.
* **Apache Hadoop**
asdas
* **Apache Spark**
asdas
* **Oracle Java SE 8**
You can download java version 8 from the [oracle website](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html).
* **Apache Maven**
You can download mvn version 3.0.4 from the [Maven Releases](https://maven.apache.org/docs/3.0.4/release-notes.html).
* **Eclipse IDE** [Alternatively, any other IDE you want]

## Deploy Project

After setting up all essential tools, then you are in a linux os with java, mvn and hadoop-spark. First, you have to do 
```sh
$ git clone https://github.com/stevpla/BigDataTaxiApplication.git
```
to download project. Before you build the projects, you can edit two yml files located at (```/HadoopSparkFramework/src/main/resources/configurations/```)
* **hdfs_csv_path.yml**
An example could be:
```
hdfs: hdfs://quickstart.cloudera:8020//cloudera//
csv: fares.csv
```
* **tcp_server.yml**
In case of streaming mode, you can define the ip address and port of the TCP server

The, you have to build the two java maven projects (pom.xml) with mvn tool. Below are the necessary steps to build:
1. mvn build
2. mvn clean
3. mvn other

There are two types of queries to run. Batch and streaming. 
### Batch
In the path:
```sh
HadoopSparkFramework/src/main/resources/shscripts/run_batch_question.sh
```
there is a shell script you can execute to run every query. There are 8 queries as mentioned in exercise. So, for example you could run the first query, as:
```sh
$ ./run_batch_question.sh 1
```
which 1 is the number of question. See the script for more details.
### Streaming
In the path:
```sh
HadoopSparkFramework/src/main/resources/shscripts/run_streaming_question.sh
```
there is a shell script for run streamings questions (3,4,6).
So, for example you could run the first query, as:
```sh
$ ./run_streaming_question.sh 3
```
which 3 is the number of question. See the script for more details.


## docker-spark-cluster
Build your own Spark cluster setup in Docker.      
A multinode Spark installation where each node of the network runs in its own separated Docker container.   
The installation takes care of the Hadoop & Spark configuration, providing:
1) a debian image with scala and java (scalabase image)
2) four fully configured Spark nodes running on Hadoop (sparkbase image):
    * nodemaster (master node)
    * node2      (slave)
    * node3      (slave)
    * node4      (slave)

### Motivation
You can run Spark in a (boring) standalone setup or create your own network to hold a full cluster setup inside Docker instead.   
I find the latter much more fun:
* you can experiment with a more realistic network setup
* tweak nodes configuration
* simulate scalability, downtimes and rebalance by adding/removing nodes to the network automagically   

There is a Medium article related to this: https://medium.com/@rubenafo/running-a-spark-cluster-setup-in-docker-containers-573c45cceabf

### Installation
1) Clone this repository
2) cd scalabase
3) ./build.sh    # This builds the base java+scala debian container from openjdk9
4) cd ../spark
5) ./build.sh    # This builds sparkbase image
6) run ./cluster.sh deploy
7) The script will finish displaying the Hadoop and Spark admin URLs:
    * Hadoop info @ nodemaster: http://172.18.1.1:8088/cluster
    * Spark info @ nodemaster : http://172.18.1.1:8080/
    * DFS Health @ nodemaster : http://172.18.1.1:9870/dfshealth.html

### Options
```bash
cluster.sh stop   # Stop the cluster
cluster.sh start  # Start the cluster
cluster.sh info   # Shows handy URLs of running cluster

# Warning! This will remove everything from HDFS
cluster.sh deploy # Format the cluster and deploy images again
```

### Inside Docker Container
All the above are mentioned in the repositoru of **rubenafo**. Now when all these steps are done, then we have to log in to container, copy csv and java projects and run them. So the steps basically are:
1.   Log on to the container (masterNode)
``` sh
$ sudo docker exec -it <containerID> bash
```
2. Enable hdfs command
``` sh
$ cd /home/hadoop
$ source .bashrc
```
3. Copy csv file from local host to container
``` sh
$ sudo docker cp fares.csv <containerID>:/home/
```
4. Pass csv from container to hdfs system of docker cluster
```
$ hdfs namenode -format
$ su - hadoop
$ hdfs dfs -put fares.csv hdfs://nodemaster:9000//fares.csv`
```
5. Copy java projects (x2) from local host to container
```
$ sudo docker cp <path_to_java_project> <container_id>:/home
```
6. Docker Containers does not have Maven installed, so you have to install it at your own
```
$ apt install maven
```
7. Build projects with mvn
```
$ mvn clean install
$ mvn build
```
9. Run scripts for each questions [See ##Deploy Project section]


