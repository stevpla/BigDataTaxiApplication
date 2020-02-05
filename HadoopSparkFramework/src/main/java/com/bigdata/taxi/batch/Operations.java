package com.bigdata.taxi.batch;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.asc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import com.bigdata.taxi.functions.CustomUDF;

public class Operations {

	private SparkSession sparkSession;

	public Operations(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}

	/*
	 * Decouple plan followed. Construct first the dataset, and then select
	 * (express what want to see) from this dataset.
	 */

	public Dataset<Row> constructDSAndFindNoTaxiRoutesPerDayPerArea(
			Dataset<Row> ds) {
		// Add new column to existing dataframe, with only date part from column
		// "pickup_datetime".
		Dataset<Row> ds2 = ds.withColumn("pickup_date",
				date_format(col("pickup_datetime"), "yyyy-MM-dd"));
		// Register UDF2 method.
		sparkSession.udf().register("areaSplit", CustomUDF.areaSplit,
				DataTypes.IntegerType);
		// Add new column Area from passenger_count, because it is int type.
		return ds2.withColumn(
				"area",
				callUDF("areaSplit", col("pickup_longitude"),
						col("pickup_latitude")));
	}

	public void selectFromDSAndFindNoTaxiRoutesPerDayPerArea(Dataset<Row> ds) {
		// Now group by day and by area
		ds.orderBy("pickup_date", "area").groupBy("pickup_date", "area")
				.count().show();
	}

	public Dataset<Row> constructDSAndFindMaxDurationAndDistanceRouteInArea(
			Dataset<Row> ds) {
		// Area which taxi route with largest distance.
		// Here, we have to create new column with the calculated distance first
		// Register UDF4 method
		sparkSession.udf().register("haversineFormula",
				CustomUDF.haversineFormula, DataTypes.DoubleType);
		// Create the dataframe
		return ds.withColumn(
				"distance",
				callUDF("haversineFormula", col("pickup_longitude"),
						col("pickup_latitude"), col("dropoff_longitude"),
						col("dropoff_latitude")));

	}

	public void selectFromDSAndFindMaxDurationRouteInArea(Dataset<Row> ds) {
		// Area which taxi route found with largest trip_duration.
		ds.groupBy("area").avg("trip_duration")
				.sort(desc("avg(trip_duration)")).show(1);
	}

	public void selectFromDSAndFindMaxDistanceRouteInArea(Dataset<Row> ds) {
		ds.groupBy("area").avg("distance").sort(desc("avg(distance)")).show(1);
	}

	public Dataset<Row> selectFromDSAndFindRoutesWhichDistanceGreater1KmWhichDurationGreater10MinAndWhichPassengersGreater2(
			Dataset<Row> df, int streaming_mode) {
		// trip_duration convert to min. 10min -> 600sec
		if (streaming_mode == 0) {
			df.filter(
					col("distance").$greater(1).and(
							col("trip_duration").$greater(600).and(
									col("passenger_count").$greater(2))))
					.show();
			return null;
		} else if (streaming_mode == 1)
			return df.filter(col("distance").$greater(1).and(
					col("trip_duration").$greater(600).and(
							col("passenger_count").$greater(2))));

		return null;
	}

	public void selectFromDSAndFindTaxiRoutesPerHourFromStart(Dataset<Row> ds) {
		ds.groupBy(functions.window(col("pickup_datetime"), "1 hour")).count()
				.sort(asc("window")).show();
	}

	public Dataset<Row> constructDSAndAndFindHowManyTaxiTripsStartedInLastHourInThisArea(
			Dataset<Row> ds, String hour) {
		// Find previous hour
		String[] array_of_hour = hour.split(":");
		String last_hour;
		int x = Integer.parseInt(array_of_hour[0]);
		if (x == 00) {
			last_hour = String.valueOf("23") + ":" + array_of_hour[1];
		} else {
			last_hour = String.valueOf(x - 1) + ":" + array_of_hour[1];
		}
		// Register UDF10 method
		sparkSession.udf().register("pointInArea", CustomUDF.pointInArea,
				DataTypes.BooleanType);
		// Create pickup_time column to compare hour and last hour
		Dataset<Row> ds2 = ds.withColumn("pickup_time",
				date_format(col("pickup_datetime"), "HH:mm:ss"));
		Dataset<Row> ds3 = ds2.orderBy("pickup_datetime").filter(
				col("pickup_time").$greater(last_hour).and(
						col("pickup_time").$less(hour)));
		// Now, Check for every taxi route if started inside area
		Dataset<Row> ds4 = ds3.withColumn(
				"index_hour_area",
				callUDF("pointInArea", col("pickup_longitude"),
						col("pickup_latitude")));
		// If taxi route belongs to area, then in index_hour_area col there will
		// be a true flag.
		return ds4.filter(col("index_hour_area").equalTo(true));
	}

	public void selectFromDSAndFindHowManyTaxiTripsStartedInLastHourInThisArea(
			Dataset<Row> ds) {
		// Group By datetime and now we have already the 1 hour, just make the
		// window and count.
		ds.groupBy(functions.window(col("pickup_datetime"), "1 hour")).count()
				.sort(asc("window")).show();
	}

	public Dataset<Row> constructDSAndAndFindPerDayHourWhichVendorHasMostTaxiRoutes(
			Dataset<Row> ds) {
		// Register UDFs methods
		sparkSession.udf().register("dateFormatParsing",
				CustomUDF.dateFormatParsing, DataTypes.StringType);
		sparkSession.udf().register("hourFormatParsing",
				CustomUDF.hourFormatParsing, DataTypes.StringType);
		// Clone in 2 same dataframes. Split one dataframe with all vendors to 2
		// dataframes.
		Dataset<Row> ds_vendor_1 = ds.filter(col("vendor_id").equalTo(1));
		Dataset<Row> ds_vendor_2 = ds.filter(col("vendor_id").equalTo(2));
		// Create 2 more columns, with pure date and hour in order to make the
		// grouping. Thats because, grouping
		// with pickup_date is hard. Its type is start:timestamp-end:timestamp.
		// Make this for 2 datasets.
		Dataset<Row> f_vendor_1 = ds_vendor_1
				.groupBy(functions.window(col("pickup_datetime"), "1 hour"))
				.count()
				.withColumn("t_date",
						callUDF("dateFormatParsing", col("window")))
				.withColumn("t_hour",
						callUDF("hourFormatParsing", col("window")));
		Dataset<Row> f_vendor_2 = ds_vendor_2
				.groupBy(functions.window(col("pickup_datetime"), "1 hour"))
				.count()
				.withColumn("t_date",
						callUDF("dateFormatParsing", col("window")))
				.withColumn("t_hour",
						callUDF("hourFormatParsing", col("window")));

		// Register Dataset as a SQL temporary view
		f_vendor_1.createOrReplaceTempView("Vend1");
		f_vendor_2.createOrReplaceTempView("Vend2");

		// https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group
		Dataset<Row> final_vendor_1_ds = sparkSession
				.sql("select t_date, t_hour, count from  (select *, row_number() OVER (PARTITION BY t_date ORDER BY count DESC) as rn FROM Vend1) tmp where rn = 1");
		// final_vendor_1_ds.orderBy("t_date", "t_hour").show();
		// For vendor 2
		Dataset<Row> final_vendor_2_ds = sparkSession
				.sql("select t_date, t_hour, count from  (select *, row_number() OVER (PARTITION BY t_date ORDER BY count DESC) as rn FROM Vend2) tmp where rn = 1");
		// final_vendor_2_ds.orderBy("t_date", "t_hour").show();

		// Add vendor to each dataset before the end action, because vendor_id
		// column has been removed from grouping.
		Dataset<Row> t_final_vendor_1_ds = final_vendor_1_ds.withColumn(
				"vendor", functions.lit((1)));
		Dataset<Row> t_final_vendor_2_ds = final_vendor_2_ds.withColumn(
				"vendor", functions.lit((2)));

		// Merge 2 dataframes with union.
		return t_final_vendor_1_ds.union(t_final_vendor_2_ds);
	}

	public void selectFromDSAndAndFindPerDayHourWhichVendorHasMostTaxiRoutes(
			Dataset<Row> ds) {
		ds.orderBy("t_date").show();
	}

	public Dataset<Row> constructDSAndAndFindHowManyTaxiCalledBySundaySaturdayByHour(
			Dataset<Row> ds) {
		return ds.withColumn("pickup_date",
				date_format(col("pickup_datetime"), "yyyy-MM-dd")).
		// dayofweek(date) returns 1 if date is Sunday day or 7 if date
		// is Saturday day.
				filter(functions.dayofweek(col("pickup_date")).equalTo(1)
						.or(functions.dayofweek(col("pickup_date")).equalTo(7)));
	}

	public void selectFromDSAndAndFindHowManyTaxiCalledBySundaySaturdayByHour(
			Dataset<Row> ds) {
		ds.groupBy(functions.window(col("pickup_datetime"), "1 hour")).count()
				.orderBy("window").show();
	}

	public Dataset<Row> constructDSAndAndFindHowManyTaxiCalledByMondayByHour(
			Dataset<Row> ds) {
		return ds.withColumn("pickup_date",
				date_format(col("pickup_datetime"), "yyyy-MM-dd")).
		// dayofweek(date) returns 2 if Monday
				filter(functions.dayofweek(col("pickup_date")).equalTo(2));
	}

	public void selectFromDSAndAndFindHowManyTaxiCalledByMondayByHour(
			Dataset<Row> ds) {
		ds.groupBy(functions.window(col("pickup_datetime"), "1 hour")).count()
				.orderBy("window").show();
	}
}
