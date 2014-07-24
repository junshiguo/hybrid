package utility;

public class test {
	public static void main(String[] args){
		int[] a = {1,2,3,4,5};
		fun(a);
		System.out.println(a[0]);
	}
	
	public static void fun(int[] a){
		a[0] = 100;
	}

}
