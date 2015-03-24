package hybridUtility;

import hybridConfig.HConfig;

import java.util.ArrayList;

public class Knapsack {
	public static void main(String[] args){
		
	}
	
	public static ArrayList<Integer> findTenantsToOffload(ArrayList<Integer> activeList, int voltdbSize, int workload){
		int activeNumber = activeList.size();
		if(activeNumber <= 0) return null;
		int benefit[][] = new int[activeNumber][voltdbSize+1];
		int pre[][] = new int[activeNumber][voltdbSize+1];
		for(int i = 0; i < activeNumber; i++){
			for(int j = 0; j < voltdbSize; j++){
				benefit[i][j] = 0;
				pre[i][j] = 0;
			}
		}
		int wl = HConfig.getQT(activeList.get(0), false);
		for(int i = (int) Math.round(HConfig.getDS(activeList.get(0), false)); i < voltdbSize; i++){
			benefit[0][i] = wl;
			pre[0][i] = 1;
		}
		for(int i = 1; i < activeNumber; i++){
			int ds = (int) Math.round(HConfig.getDS(activeList.get(i), false));
			int slo = HConfig.getQT(i, false);
			for(int j = 0; j <= voltdbSize; j++){
				benefit[i][j] = benefit[i-1][j];
				pre[i][j] = 0;
				if(j >= ds && benefit[i-1][j-ds]+slo > benefit[i][j]){
					benefit[i][j] = benefit[i-1][j-ds]+slo;
					pre[i][j] = 1;
				}
			}
		}
		int j;
		for(j = 0; j <= voltdbSize; j++){
			if(benefit[activeNumber][j] >= workload){
				break;
			}
		}
		ArrayList<Integer> ret = new ArrayList<Integer>();
		int i = activeNumber - 1;
		double offloadSize = 0;
		while(i>=0){
			if(pre[i][j] == 1){
				ret.add(i);
				offloadSize += (int) Math.round(HConfig.getDS(activeList.get(0), false));
			}
			i --;
		}
		System.out.println("Number of tenant to offload: "+ret.size()+"; Total offloaded data size: "+offloadSize+"MB.");
		return ret;
	}
	
}
