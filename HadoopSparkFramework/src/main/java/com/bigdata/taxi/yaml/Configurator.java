package com.bigdata.taxi.yaml;

import java.io.File;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;


public class Configurator {
	public static void readTcpServerYML() {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		try {
			mapper.readValue(new File(
					"./src/main/resources/configurations/tcp_server.yml"),
					TcpServerInfo.class);
		} catch (Exception e) {
			System.out.println(e.toString());
			System.exit(-1);
		}
	}
	
	public static void readCsvHdfsYML() {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		//mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		try {
			mapper.readValue(new File(
					"./src/main/resources/configurations/test.yml"),
					CsvPath.class);
		} catch (Exception e) {
			System.out.println(e.toString());
			System.exit(-1);
		}
	}
}
