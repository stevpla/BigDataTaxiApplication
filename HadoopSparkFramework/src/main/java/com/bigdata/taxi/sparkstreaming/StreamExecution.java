package com.bigdata.taxi.sparkstreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import org.apache.spark.sql.types.DataTypes;

import com.bigdata.taxi.SparkConnector;
import com.bigdata.taxi.batch.Operations;
import com.bigdata.taxi.functions.CustomUDF;
import com.bigdata.taxi.yaml.Configurator;
import com.bigdata.taxi.yaml.TcpServerInfo;

public class StreamExecution {

	public static void main(String[] args) {
		// Turn Off Logging.
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// Read tcp server info from yml
		Configurator.readTcpServerYML();
		// Spark Session
		// ----------------------------------------
		// Structured Streaming Guide
		// Make streaming as batch, an easy process
		// ----------------------------------------
		SparkSession spark = SparkConnector.startSparkStreaming();
		// Create Dataframe representing the stream of input lines from
		// connection to localhost:9999
		Dataset<Row> lines = spark.readStream().format("socket")
				.option("host", TcpServerInfo.getIpAddress())
				.option("port", TcpServerInfo.getPort()).load();
		
		
		
		//Operations from batch package. Contains all functionallity for run spark
		Operations op = new Operations(spark);
		
		Dataset<Row> first_ds = lines.withColumn("value2", functions.split(col("value"), ","));

		Dataset<Row> ds_as_csv = first_ds.select(col("value2").getItem(0).alias("id").cast(DataTypes.StringType),
				col("value2").getItem(1).alias("vendor_id").cast(DataTypes.StringType),
				col("value2").getItem(2).alias("pickup_datetime").cast(DataTypes.TimestampType),
				col("value2").getItem(3).alias("pickup_dropoff_datetime").cast(DataTypes.TimestampType),
				col("value2").getItem(4).alias("passenger_count").cast(DataTypes.IntegerType),
				col("value2").getItem(5).alias("pickup_longitude").cast(DataTypes.DoubleType),
				col("value2").getItem(6).alias("pickup_latitude").cast(DataTypes.DoubleType),
				col("value2").getItem(7).alias("dropoff_longitude").cast(DataTypes.DoubleType),
				col("value2").getItem(8).alias("dropoff_latitude").cast(DataTypes.DoubleType),
				col("value2").getItem(9).alias("store_and_fwd_flag").cast(DataTypes.StringType),
				col("value2").getItem(10).alias("trip_duration").cast(DataTypes.IntegerType));
		
		
		// ACTIONS 
		//4 question
		Dataset<Row> fds = ds_as_csv.groupBy(functions.window(col("pickup_datetime"), "1 hour")).count();
		
		// Start running the query
		StreamingQuery query = fds.writeStream().outputMode("update")
				.format("console").start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
