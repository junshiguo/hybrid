package hybridConfig;

public class PossionDistribution {
	final public static int NUMBER_OF_ITERATIONS = 1;

	public static int getRandomNumber(int lambda) {
		// System.out.println(lambda);
		double tot = 0;
		double p;
		for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
			p = Math.random();
			int k = 0;
			double tmpp = 0, sum = 1, eNlambda = Math.exp(-(double) lambda);
			for (; tmpp < p;) {
				// System.out.println("(" + k + "," + lambdak / fac * eNlambda
				// + ")");
				tmpp += sum * eNlambda;
				k++;
				sum = sum * lambda / k;
			}
			tot += k;
			// System.out.println(p + " " + k);
		}
		tot /= NUMBER_OF_ITERATIONS;
		return (int) tot;
	}
}
