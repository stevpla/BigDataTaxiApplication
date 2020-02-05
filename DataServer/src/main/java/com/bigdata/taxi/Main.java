package com.bigdata.taxi;

import com.bigdata.taxi.socket.TcpSender;
import com.bigdata.taxi.yaml.Configurator;

public class Main {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		//Then read yml file
		Configurator.readYML();
		//Then load csv and then automatically start ServerSocket and
		//listen to spark client
		TcpSender.createServerSocketAndListen();
	}
}
