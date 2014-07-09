package utility;

import java.util.Random;

public class Sequence {
	public int[] Sequence;
	public int next;
	public int read, write;
	public int sum;
	
	public void shuffle(){
		Random rand = new Random();
		for(int i=0; i<read; i++){
			Sequence[i] = rand.nextInt()%21;
			Sequence[i] = (Sequence[i]+21)%21;
		}
		for(int i=0; i<write; i++){
			Sequence[i+read] = rand.nextInt()%14;
			Sequence[i+read] = (Sequence[i+read]+14)%14 + 21;
		}
		rand = new Random();
		int randIndex = 0;
		for(int i=0,j=sum-1; j>0; i++,j--){
			randIndex = rand.nextInt()%(j+1);
			randIndex = (randIndex+j+1)%(j+1);
			int tmp = Sequence[randIndex+i];
			Sequence[randIndex+i] = Sequence[i];
			Sequence[i] = tmp;
		}
	}
	
	public void initSequence(int r, int w){
		read = r;
		write = w;
		sum = r+w;
		Sequence = new int[sum];
		shuffle();
		next = 0;
	}
	
	public int nextSequence(){
		int ret;
		if(next >= read+write){
			shuffle();
			next = 0;
		}
		ret = Sequence[next];
		next++;
		return ret;
	}
}
