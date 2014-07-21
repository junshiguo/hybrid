package utility;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

/*
 * �����ж���������tpcc-mysql�ĺ���������������sql�����ʱ�õ�
 */
public class Support {
	public static Random ran = new Random();
	public static int first = 1;
	public static int C, C_255 = 0, C_1023 = 0, C_8191 = 0;
	
	public static int[] Rands(int start, int total, int nn){
		int[] ret = new int[nn];
		if(nn > total){
			return null;
		}
		int count = 0;
		Random ran = new Random();
		while(count < nn){
			int tmp = ran.nextInt(total);
			boolean flag = true;
			for(int i = 0; i < count; i++){
				if(ret[i] == tmp){
					flag = false;
					break;
				}
			}
			if(flag){
				ret[count] = tmp;
				count++;
			}
		}
		for(int i = 0; i < nn; i++){
			ret[i] += start;
		}
		return ret;
	}
	/*
	 * һ����������ĺ���������tpcc-mysql����
	 */
	public static int NURand (int A, int x, int y)
	{
		if (first == 1) {
			C_255 = ran.nextInt(255);
			C_1023 = ran.nextInt(1023);
			C_8191 = ran.nextInt(8191);
			first = 0;
		}
		switch (A) {
		case 255: C = C_255; break;
		case 1023: C = C_1023; break;
		case 8191: C = C_8191; break;
		default:
			return 0;
		}
		return (int) (((ran.nextInt(A) | ran.nextInt(y-x)+x) + C) % (y-x+1)) + x;
	}
	/*
	 * ����������last_name�ֶ�
	 */
	public static String Lastname(int num){
		String ret = null;
		String[] n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		ret = n[num/100];
		ret += n[(num/10)%10];
		ret += n[num%10];
		return ret;
	}
	/*
	 * ���[min,max]�ڵ������
	 */
	public static int RandomNumber(int min,int max){
		if(min == max) return min;
		return ran.nextInt(max-min+1)+min;
	}
	/*
	 * �����ɳ�����[x,y]�ڵ��ַ��ַ���0-9a-zA-Z���
	 */
	public static String MakeAlphaString(int x, int y){
		String alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		int arrmax = 61;  /* index of last array element */
		int i=0,len= RandomNumber(x, y);
		String ret = "";
		for (i = 0; i < len; i++){
			ret += alphanum.charAt(RandomNumber(0, arrmax));
		}
		return ret;
	}
	/*
	 * �����ɳ�����[x,y]�ڵ��ַ��ַ���0-9���
	 */
	public static String MakeNumberString (int x, int y)
	{
		String numeric = "0123456789";
		int arrmax = 9;
		int i, len;
		String str = "";
		len = RandomNumber(x, y);
		for (i = 0; i < len; i++)
			str += numeric.charAt(RandomNumber(0, arrmax));
		return str;
	}
	
	public static Long getAverage(ArrayList<Long> list){
		Long sum = new Long(0);
		if(!list.isEmpty()){
			for(Long n : list){
				sum += n;
			}
			return sum/list.size();
		}
		return sum;
	}
	
	public static Long getAverage(ArrayList<Long> list, int number){
		Long sum = new Long(0);
		if(!list.isEmpty()){
			int i = 0;
			for(Long n : list){
				sum += n;
				i++;
				if(i >= number){
					break;
				}
			}
			return sum/list.size();
		}
		return sum;
	}
	
	public static String getTimeStamp(){
		java.util.Date  date=new java.util.Date();
		String ret = new Timestamp(date.getTime()).toString();
		int ooo = ret.lastIndexOf(".");
		if(ooo != -1) ret.substring(0, ooo);
		return ret;
	}
	
}
