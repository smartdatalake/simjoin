package eu.smartdatalake.simjoin.sets;

import java.util.Arrays;

/**
 * Represents a set after transforming string tokens to integers.
 *
 */
public class IntSet implements Comparable<IntSet> {

	public String id;
	public int[] tokens;
	public double weight;

	public IntSet() {
	}

	public IntSet(String id, int[] tokens, double weight) {
		this.id = id;
		this.tokens = tokens;
		this.weight = weight;
	}

	public int compareTo(IntSet s) {

		// return Integer.compare(this.tokens.length, s.tokens.length);
		int r = this.tokens.length - s.tokens.length;

		int i = 0;
		while (r == 0 && i < this.tokens.length) {
			r = this.tokens[i] - s.tokens[i];
			i++;
		}

		if (r != 0) {
			return r;
		} else {
			return this.id.compareTo(s.id);
		}
	}

	@Override
	public String toString() {
		return id + ": " + Arrays.toString(tokens);
	}
}