package com.bigdata.taxi.functions;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;

public class CustomUDF {

	private static Point[] polygon;

	private static Integer calculateRouteArea(Double x, Double y) {
		if (x < -73.97341957 && y > 40.750942) {
			return 1;
		}
		if (x < -73.97341957 && y < 40.750942) {
			return 4;
		}
		if (x > -73.97341957 && y > 40.750942) {
			return 2;
		}
		if (x > -73.97341957 && y < 40.750942) {
			return 3;
		}
		return 0;
	}

	private static Double calculateDistanceHaversineFormula(Double lon1,
			Double lat1, Double lon2, Double lat2) {
		// distance between latitudes and longitudes
		double dlat = Math.toRadians(lat2 - lat1);
		double dlon = Math.toRadians(lon2 - lon1);

		// convert to radians
		lat1 = Math.toRadians(lat1);
		lat2 = Math.toRadians(lat2);

		double a = Math.pow(Math.sin(dlat / 2), 2)
				+ Math.pow(Math.sin(dlon / 2), 2) * Math.cos(lat1)
				* Math.cos(lat2);
		double rad = 6371;
		double c = 2 * Math.asin(Math.sqrt(a));
		// In km
		return rad * c;
	}

	public static void setPolugon(Double lon1, Double lat1, Double lon2,
			Double lat2, Double lon3, Double lat3, Double lon4, Double lat4) {
		// Create array
		polygon = new Point[4];
		polygon[0] = new Point(lon1, lat1);
		polygon[1] = new Point(lon2, lat2);
		polygon[2] = new Point(lon3, lat3);
		polygon[3] = new Point(lon4, lat4);
	}

	private static Point[] getPolygon() {
		return polygon;
	}

	private static Boolean decideWhetherATaxiRouteBelongsToAnArea(
			Double lon_taxi, Double lat_taxi) {
		Point[] tmp_polygon = getPolygon();

		int intersections = 0;
		Point prev = tmp_polygon[tmp_polygon.length - 1];
		for (Point next : tmp_polygon) {
			if ((prev.getY() <= lat_taxi && lat_taxi < next.getY())
					|| (prev.getY() >= lat_taxi && lat_taxi > next.getY())) {
				double dy = next.getY() - prev.getY();
				double dx = next.getX() - prev.getX();
				double x = (lat_taxi - prev.getY()) / dy * dx + prev.getX();
				if (x > lon_taxi) {
					intersections++;
				}
			}
			prev = next;
		}

		return intersections % 2 == 1;
	}

	private static String parseDateFromColumn(Object tmp) {
		// Input is as start:timestamp-end:timestamp format.
		// |01-03-2016 13:15:00 02-03-2016 12:00:00|
		return tmp.toString().substring(1, 11);
	}

	private static String parseStartAndEndHoursFromColumn(Object tmp) {
		return tmp.toString().substring(12, 17) + "-"
				+ tmp.toString().substring(34, 39);
	}

	/*
	 * --------------------------------------------------------------------------
	 * ---------------------------- UDF Methods
	 * ----------------------------------
	 * --------------------------------------------------------------------
	 */

	// UDF2 for 2 parameters. Not UDF1.
	public static UDF2<Double, Double, Integer> areaSplit = (x, y) -> CustomUDF
			.calculateRouteArea(x, y);

	/* https://en.wikipedia.org/wiki/Haversine_formula */
	public static UDF4<Double, Double, Double, Double, Double> haversineFormula = (
			lon1, lat1, lon2, lat2) -> CustomUDF
			.calculateDistanceHaversineFormula(lon1, lat1, lon2, lat2);

	public static UDF2<Double, Double, Boolean> pointInArea = (px, py) -> CustomUDF
			.decideWhetherATaxiRouteBelongsToAnArea(px, py);

	public static UDF1<Object, String> dateFormatParsing = (x) -> CustomUDF
			.parseDateFromColumn(x);

	public static UDF1<Object, String> hourFormatParsing = (x) -> CustomUDF
			.parseStartAndEndHoursFromColumn(x);
}
