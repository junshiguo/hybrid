package hybridTest;

import java.util.Random;

public class HSequence {
	public static int DISTRICT = 0;
	public static int ITEM = 1;
	public static int STOCK = 2;
	public static int WAREHOUSE = 3;
	public static int CUSTOMER = 4;
	public static int ORDER_LINE = 5;
	public static int NEW_ORDERS = 6;
	public static int ORDERS = 7;
	public static int HISTORY = 8;
	public static int[] readTable = {3,1,2,1,8,3,1,2,0};
	public static int[] writeTable = {2,0,1,1,3,2,2,2,1};
	public static int[][] requestDistrict = {{0,4,16},{25,28}}; //0
	public static int[][] requestItem = {{1},{}}; //1
	public static int[][] requestStock = {{2,18},{26}}; //2
	public static int[][] requestWarehouse = {{3},{27}}; //3
	public static int[][] requestCustomer = {{5,6,7,8,9,10,11,19},{29,30,33}}; //4
	public static int[][] requestOrderLine = {{12,15,17},{23,32}}; //5
	public static int[][] requestNewOrders = {{13},{22,34}}; //6
	public static int[][] requestOrders = {{14,20},{21,31}}; //7
	public static int[][] requestHistory = {{},{24}}; //8
	public static int[][][] request = {requestDistrict, requestItem, requestStock, requestWarehouse, requestCustomer, requestOrderLine,
		requestNewOrders, requestOrders, requestHistory};
	
	public int[] sequence;
	public int[] tables;
	public int next;
	public int w,r,sum = 300;
	public double writePercent;
	public int connNumber;
	
	public static void main(String[] args){
		HSequence seq = new HSequence(0.5, 5);
		for(int i=0; i<300; i+=5){
			System.out.println(seq.tables[i]+" "+seq.tables[i+1]+" "+seq.tables[i+2]+" "+seq.tables[i+3]+" "+seq.tables[i+4]);
			System.out.println(seq.sequence[i]+" "+seq.sequence[i+1]+" "+seq.sequence[i+2]+" "+seq.sequence[i+3]+" "+seq.sequence[i+4]+"\n");
		}
	}
	
	public HSequence(double writePercent, int connNumber){
		this.connNumber = connNumber;
		this.writePercent = writePercent;
		initSequence();
	}
	
	public void setWritePercent(double newWP){
		this.writePercent = newWP;
	}
	
	public void initSequence(){
		Random rand = new Random();
		int dwtmp = rand.nextInt(11);
		double dw = (dwtmp-5)/100.0;
		double actualWP = writePercent + dw; //dw: +-0.05
		w = (int) (sum*actualWP);
		r = 300 - w;
		sequence = new int[sum];
		tables = new int[sum];
		for(int i = 0; i < w; i++)	{
			sequence[i] = 1; //1 for write
			tables[i] = -1;
		}
		for(int i = w; i < sum; i++){
			sequence[i] = 0; //0 for read
			tables[i] = -1;
		}
		//******************shuffle*************************//
		rand = new Random();
		int randIndex = 0;
		for(int i=0,j=sum-1; j>0; i++,j--){
			randIndex = rand.nextInt(j+1);
			int tmp = sequence[randIndex+i];
			sequence[randIndex+i] = sequence[i];
			sequence[i] = tmp;
		}
		//************give actual values to sequence*****************//
		for(int i = 0; i < sum; i+=connNumber){
			int count = 0;
			while(count < connNumber){
				int n = rand.nextInt(9);
				boolean flag = true;
				for(int j = 0; j < count; j++){
					if(n == tables[i+j]){
						flag = false;
						break;
					}
				}
				if(flag == true && ((sequence[i+count]==1 && writeTable[n]!=0) || (sequence[i+count] == 0 && readTable[n]!=0))){
					tables[i+count] = n;
//					sequence[i+count] = request[n][1][rand.nextInt(writeTable[n])];
					count++;
				}
			}
			for(int j = 0; j < connNumber; j ++){
				int t = tables[i+j];
				if(sequence[i+j] == 0){
					sequence[i+j] = request[t][0][rand.nextInt(readTable[t])];
				}else if(sequence[i+j] == 1){
					sequence[i+j] = request[t][1][rand.nextInt(writeTable[t])];
				}
			}
		}
		//************inti next****************//
		next = 0;
	}
	
	public int next(){
		int ret;
		if(next >= sum){
			initSequence();
			next = 0;
		}
		ret = sequence[next];
		next++;
		return ret;
	}

}
