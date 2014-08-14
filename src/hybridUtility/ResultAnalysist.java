package hybridUtility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import utility.Support;

public class ResultAnalysist {
	public static void main(String[] args) throws IOException{
		String path = "../result3000.8/";
		FileWriter fstream = new FileWriter(path+"HResult.txt");
		BufferedWriter writer = new BufferedWriter(fstream);
		BufferedReader[] reader = new BufferedReader[18];
		for(int i = 0; i < 18; i++)
			reader[i] = new BufferedReader(new FileReader(path+"HTest3000."+i+".txt"));
		String str;
		while((str = reader[0].readLine()) != null){
			String[] line = str.split(" ");
			int time = Integer.parseInt(line[0]);
//			double[] avg = new double[18];
//			avg[0] = Double.parseDouble(line[1]);
			long tp = Long.parseLong(line[2]);
			ArrayList<Double> timePerQuery = new ArrayList<Double>();
			for(int j = 3; j < line.length; j++){
				timePerQuery.add(Double.parseDouble(line[j]));
			}
			for(int i = 1; i < 18; i++){
				str = reader[i].readLine();
				line = str.split(" ");
//				avg[i] = Double.parseDouble(line[1]);
				tp += Long.parseLong(line[2]);
				for(int j = 3; j < line.length; j++){
					timePerQuery.add(Double.parseDouble(line[j]));
				}
			}
			Collections.sort(timePerQuery, new Comparator<Double>(){
				@Override
				public int compare(Double o1, Double o2) {
					return o1.compareTo(o2);
				}
			});
			long totalQuery = timePerQuery.size();
			if(totalQuery != 0)	{
				writer.write(""+time+" "+Support.getDoubleAverage(timePerQuery)+" "+tp);
			writer.write(" "+timePerQuery.get((int) (totalQuery*0.9)));
			writer.write(" "+timePerQuery.get((int) (totalQuery*0.95)));
			writer.write(" "+timePerQuery.get((int) (totalQuery*0.99)));
			writer.newLine(); writer.flush();
			}
		}
		writer.close();
		for(int i = 0; i < 18; i++)	reader[i].close();
	}

}
