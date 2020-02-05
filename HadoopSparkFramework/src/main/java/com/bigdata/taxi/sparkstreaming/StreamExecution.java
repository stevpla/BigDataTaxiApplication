package com.bigdata.taxi.sparkstreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.types.DataTypes;

import com.bigdata.taxi.SparkConnector;
import com.bigdata.taxi.batch.Operations;
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
		StreamingQuery query = null;

		// Operations from batch package. Contains all functionallity for run
		// spark
		Operations op = new Operations(spark);

		// -------------------------------------------------------------------------------------------------------
		Dataset<Row> first_ds = lines.withColumn("value2",
				functions.split(col("value"), ","));
		// Create schema
		Dataset<Row> ds_as_csv = first_ds
				.select(col("value2").getItem(0).alias("id")
						.cast(DataTypes.StringType),
						col("value2").getItem(1).alias("vendor_id")
								.cast(DataTypes.StringType),
						col("value2").getItem(2).alias("pickup_datetime")
								.cast(DataTypes.TimestampType),
						col("value2").getItem(3)
								.alias("pickup_dropoff_datetime")
								.cast(DataTypes.TimestampType),
						col("value2").getItem(4).alias("passenger_count")
								.cast(DataTypes.IntegerType),
						col("value2").getItem(5).alias("pickup_longitude")
								.cast(DataTypes.DoubleType),
						col("value2").getItem(6).alias("pickup_latitude")
								.cast(DataTypes.DoubleType),
						col("value2").getItem(7).alias("dropoff_longitude")
								.cast(DataTypes.DoubleType),
						col("value2").getItem(8).alias("dropoff_latitude")
								.cast(DataTypes.DoubleType),
						col("value2").getItem(9).alias("store_and_fwd_flag")
								.cast(DataTypes.StringType),
						col("value2").getItem(10).alias("trip_duration")
								.cast(DataTypes.IntegerType));
		// -------------------------------------------------------------------------------------------------------

		// Check for argument. For argument = 3, run 3rd question in streaming
		// mode
		if (args[0].equals("3")) {

		} else if (args[0].equals("4")) {
			Dataset<Row> fds = op
					.selectFromDSAndFindTaxiRoutesPerHourFromStart(ds_as_csv, 1);
			// Start running the query
			query = fds.writeStream().outputMode("update").format("console")
					.start();
		} else if (args[0].equals("6")) {

		}

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
