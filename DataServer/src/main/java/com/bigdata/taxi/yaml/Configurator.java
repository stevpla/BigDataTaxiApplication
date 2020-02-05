package com.bigdata.taxi.yaml;

import java.io.File;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class Configurator {

	public static void readYML() {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		try {
			mapper.readValue(new File(
					"./src/main/resources/configurations/server_info.yml"),
					ServerInfo.class);
			// System.out.println(ServerInfo.getPort());

		} catch (Exception e) {
			System.out.println(e.toString());
			System.exit(-1);
		}
	}
}
