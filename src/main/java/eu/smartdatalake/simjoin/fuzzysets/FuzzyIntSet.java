package eu.smartdatalake.simjoin.fuzzysets;

import java.util.Arrays;

/**
 * Represents a fuzzy set after transforming string tokens to integers.
 *
 */
public class FuzzyIntSet implements Comparable<FuzzyIntSet> {

	public String id;
	public int[][] tokens;
	public double weight;

	public int compareTo(FuzzyIntSet s) {
		return Integer.compare(this.tokens.length, s.tokens.length);
	}

	@Override
	public String toString() {
		return id + ": " + Arrays.deepToString(tokens);
	}
}