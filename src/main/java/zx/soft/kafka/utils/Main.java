package zx.soft.kafka.utils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;

public class Main {

	public static void main(String[] args) throws Exception {
		byte[] b = new byte[1024];
		FileInputStream in = new FileInputStream("src/main/resources/kafka.properties");
        BufferedInputStream bin = new BufferedInputStream(in);
        DataInputStream din = new DataInputStream(bin);
        din.read(b);
        System.out.println(b);
        din.close();
	}

}
