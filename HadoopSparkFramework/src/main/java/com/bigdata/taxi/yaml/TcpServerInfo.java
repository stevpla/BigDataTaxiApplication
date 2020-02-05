package com.bigdata.taxi.yaml;

import java.util.Map;

public class TcpServerInfo {
	private static Map<String, String> server;

	public void setServer(Map<String, String> server) {
		this.server = server;
	}

	public static String getIpAddress() {
		return server.get("ip_address");
	}

	public static int getPort() {
		return Integer.parseInt(server.get("port"));
	}

	public static Map<String, String> getServer() {
		return server;
	}

}
