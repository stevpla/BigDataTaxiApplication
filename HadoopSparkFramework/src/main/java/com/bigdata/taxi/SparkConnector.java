package com.bigdata.taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.bigdata.taxi.yaml.CsvPath;

public class SparkConnector {

	private static JavaSparkContext sc;

	public static SparkSession startSparkConnection() {
		SparkConf conf = new SparkConf();
		conf.setAppName("My 1st Spark app");
		conf.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		// The entry point to all functionality in Spark is the SparkSession
		return SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
	}

	public static SparkSession startSparkStreaming() {
		return SparkSession.builder().appName("JavaSApp2").master("local[*]")
				.getOrCreate();
	}

	public static Dataset<Row> readCsvFromHdfsSource(SparkSession ss) {
		// Now read csv , from hdfs source
		// [cloudera@quickstart ~]$ hdfs dfs -put
		// /home/cloudera/Desktop/fares.csv
		// hdfs://quickstart.cloudera:8020//user//cloudera//fares.csv
		return ss.read().option("header", true).option("inferSchema", "true")
				.option("timestampFormat", "yyyy-MM-dd hh:mm:ss")
				.csv(CsvPath.getHdfs() + CsvPath.getCsv());
	}

	public static void closeJSC() {
		// Close Spark context
		sc.close();
	}
}
