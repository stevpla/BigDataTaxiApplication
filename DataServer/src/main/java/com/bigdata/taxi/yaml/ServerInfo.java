package com.bigdata.taxi.yaml;

import java.util.Map;

public class ServerInfo {
	private static String domain;
	private static Map<String, String> server;

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setServer(Map<String, String> server) {
		this.server = server;
	}

	public static String getIpAddress() {
		return server.get("ip_address");
	}

	public static int getPort() {
		return Integer.parseInt(server.get("port"));
	}

	public static String getDomain() {
		return domain;
	}

	public static Map<String, String> getServer() {
		return server;
	}
}
