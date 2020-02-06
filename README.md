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

## Docker Spark Cluster

Publishing in StackEdit makes it simple for you to publish online your files. Once you're happy with a file, you can publish it to different hosting platforms like **Blogger**, **Dropbox**, **Gist**, **GitHub**, **Google Drive**, **WordPress** and **Zendesk**. With [Handlebars templates](http://handlebarsjs.com/), you have full control over what you export.

> Before starting to publish, you must link an account in the **Publish** sub-menu.

