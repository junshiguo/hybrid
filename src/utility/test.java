package utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class test {
	
	public static void main(String[] args) throws IOException{
		String path = ".";
		int fileNumber = 3;
		BufferedWriter writer = new BufferedWriter(new FileWriter(path+"/test.txt", false));
		BufferedReader[] reader = new BufferedReader[fileNumber];
		for(int i = 0; i < fileNumber; i++){
			reader[i] = new BufferedReader(new FileReader(path+"/test"+i+".txt"));
		}
		double wp = 0.0;
		for(int i = 0; i < 51; i++){
			long tp = 0;
			for(int j = 0; j < fileNumber; j++){
				String[] info = reader[j].readLine().split(" ");
				tp += Long.parseLong(info[1].trim());
			}
			tp /= fileNumber;
			writer.write(wp+" "+tp);
			writer.newLine(); writer.flush();
			wp += 0.02;
		}
		writer.close();
		for(int i = 0; i < fileNumber; i++){
			reader[i].close();
		}
	}

}
