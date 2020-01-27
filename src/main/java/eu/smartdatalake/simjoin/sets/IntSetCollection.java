package eu.smartdatalake.simjoin.sets;

import java.util.Arrays;

/**
 * Represents a collection of sets with integer tokens.
 *
 */
public class IntSetCollection {

	public int numTokens;
	public int[][] sets;
	public String[] keys;
	public double[] weights;

	public IntSetCollection(int numTokens, int len) {
		this.numTokens = numTokens;
		this.sets = new int[len][];
		this.keys = new String[len];
		this.weights = new double[len];
	}

	public void add(IntSet set, int i) {
		this.sets[i] = set.tokens;
		this.keys[i] = set.id;
		this.weights[i] = set.weight;
	}

	@Override
	public String toString() {
		return Arrays.deepToString(sets) + " " + Arrays.toString(keys);
	}
}