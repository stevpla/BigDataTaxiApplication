package com.bigdata.taxi.yaml;

import java.util.Map;


public class CsvPath {
	
	private static Map<String, String> path;

	public void setPath(Map<String, String> path) {
		this.path = path;
	}

	public static String getHdfs() {
		return path.get("hdfs");
	}

	public static String getCsv() {
		return path.get("csv");
	}

	public static Map<String, String> getPath() {
		return path;
	}
}
