package com.bigdata.taxi.socket;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.bigdata.taxi.yaml.ServerInfo;

public class TcpSender {

	public static void createServerSocketAndListen()
			throws InterruptedException {
		
		final Charset ISO = Charset.forName("ISO-8859-1");
		final Charset UTF_8 = Charset.forName("UTF-8");
		
		try {
			// Create server socket
			ServerSocket s_socket = new ServerSocket(ServerInfo.getPort());

			while (true) {
				// If connection come, then accept it in a new socket
				Socket sock = s_socket.accept();
				// Open output stream
				//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
						//sock.getOutputStream()));
				
				//PrintWriter out = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), UTF_8));
				//Writer out = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), "UTF8"));
				DataOutputStream out = new DataOutputStream(sock.getOutputStream());
				
				// Open file stream and read line by line
				// them to Spark Client App
				String line = "";
				
				String opa = null;
				try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
						"./src/main/resources/input/fares.csv"), "UTF8"))) {
					while ((line = br.readLine()) != null) {
						//String lol = new String (line.getBytes(ISO), UTF_8);
						//byte[] udfbytes = line.getBytes(ISO);
						//opa = new String(udfbytes, UTF_8);
						//System.out.println("ERROR 1");
						//System.out.println(line);
						//bw.write(line);;
						out.writeUTF(line); 
						//System.out.println("ERROR 2");
						out.flush();
						//System.out.println("ERROR 3");
						//bw.flush();
						//System.out.println();
						
					}
					
					//Close socket
					sock.close();
					out.close();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
