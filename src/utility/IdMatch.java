package utility;
 
public class IdMatch {
	public static int START[] = {0, 3000, 5000};
	public static int TOTAL[] = {3000, 2000, 2000};
	public static double PercentDS[] = {0.5, 0.3, 0.2};
	public static int TotalTenant = 3000;
	
	public static int id2TableIndex(int id){
		int start = 0, i;
		for(i = 0; i < 3; i++){
			int tmp = (int) (TotalTenant * PercentDS[i]);
			if(id < tmp + start){
				break;
			}else{
				start += tmp;
			}
		}
		return START[i] + id - start;
	}
	
	public static int tableIndex2Id(int index){
		return 0;
	}
	
}
