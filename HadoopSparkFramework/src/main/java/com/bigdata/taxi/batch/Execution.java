package com.bigdata.taxi.batch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.bigdata.taxi.batch.Operations;
import com.bigdata.taxi.functions.CustomUDF;
import com.bigdata.taxi.yaml.Configurator;
import com.bigdata.taxi.SparkConnector;

public class Execution {

	public static void main(String[] args) {
		// Read csv path into hdfs
		Configurator.readCsvHdfsYML();
		// Turn Off Logging.
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		// Spark Session
		SparkSession sparkSession = SparkConnector.startSparkConnection();
		Dataset<Row> ds = SparkConnector.readCsvFromHdfsSource(sparkSession);
		// Create Operations object to call methods for each question.
		Operations op = new Operations(sparkSession);

		// Argument is given by shell script arg.
		if (args[0].equals("1")) {
			op.selectFromDSAndFindNoTaxiRoutesPerDayPerArea(op
					.constructDSAndFindNoTaxiRoutesPerDayPerArea(ds));
		} else if (args[0].equals("2")) {
			// This question depends on 1st question dataframe.
			// -----------------------------------------------
			Dataset<Row> ds2 = op
					.constructDSAndFindMaxDurationAndDistanceRouteInArea(op
							.constructDSAndFindNoTaxiRoutesPerDayPerArea(ds));
			op.selectFromDSAndFindMaxDurationRouteInArea(ds2);
			op.selectFromDSAndFindMaxDistanceRouteInArea(ds2);
		} else if (args[0].equals("3")) {
			// /This question depends on 2nd question dataframe.
			// -----------------------------------------------
			op.selectFromDSAndFindRoutesWhichDistanceGreater1KmWhichDurationGreater10MinAndWhichPassengersGreater2(op
					.constructDSAndFindMaxDurationAndDistanceRouteInArea(op
							.constructDSAndFindNoTaxiRoutesPerDayPerArea(ds)), 0);
		} else if (args[0].equals("4")) {
			// No dependency
			op.selectFromDSAndFindTaxiRoutesPerHourFromStart(ds);
		} else if (args[0].equals("5")) {
			// No dependency
			// 9 more arguments from shell Script
			// x1 y1 x2 y2 x3 y3 x4 y4 hour
			// Reference
			CustomUDF.setPolugon(Double.parseDouble(args[1]),
					Double.parseDouble(args[2]), Double.parseDouble(args[3]),
					Double.parseDouble(args[4]), Double.parseDouble(args[5]),
					Double.parseDouble(args[6]), Double.parseDouble(args[7]),
					Double.parseDouble(args[8]));
			op.selectFromDSAndFindHowManyTaxiTripsStartedInLastHourInThisArea(op
					.constructDSAndAndFindHowManyTaxiTripsStartedInLastHourInThisArea(
							ds, args[9]));

		} else if (args[0].equals("6")) {
			// No dependency
			op.selectFromDSAndAndFindPerDayHourWhichVendorHasMostTaxiRoutes(op
					.constructDSAndAndFindPerDayHourWhichVendorHasMostTaxiRoutes(ds));
		} else if (args[0].equals("7")) {
			// No dependency
			op.selectFromDSAndAndFindHowManyTaxiCalledBySundaySaturdayByHour(op
					.constructDSAndAndFindHowManyTaxiCalledBySundaySaturdayByHour(ds));
   		} else if (args[0].equals("8")) {
			// No dependency
			op.selectFromDSAndAndFindHowManyTaxiCalledByMondayByHour(op
					.constructDSAndAndFindHowManyTaxiCalledByMondayByHour(ds));
		}

		// Close Spark context
		SparkConnector.closeJSC();
	}
}
