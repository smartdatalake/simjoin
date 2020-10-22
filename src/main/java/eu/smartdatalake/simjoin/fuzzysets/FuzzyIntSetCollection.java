package eu.smartdatalake.simjoin.fuzzysets;

import java.util.PriorityQueue;

import eu.smartdatalake.simjoin.sets.IntSet;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Represents a collection of fuzzy sets with integer tokens.
 *
 */
public class FuzzyIntSetCollection {

	public int[][][] sets;
	public String[] keys;
	public double[] weights;
	public int numTokens;

	public FuzzyIntSetCollection(int numTokens, int len) {
		this.numTokens = numTokens;
		this.sets = new int[len][][];
		this.keys = new String[len];
		this.weights = new double[len];
	}

	/**
	 * Adds a fuzzy set to the collection.
	 * 
	 * @param set
	 *            The set to be added.
	 * @param i
	 *            The position to insert the set.
	 */
	public void add(FuzzyIntSet set, int i) {
		this.sets[i] = set.tokens;
		this.keys[i] = set.id;
		this.weights[i] = set.weight;
	}

	/**
	 * @return A flattened collection of tokens.
	 */
	public IntSetCollection flatten() {
		IntSetCollection collection = new IntSetCollection(this.numTokens, sets.length);

		PriorityQueue<IntSet> tempList = new PriorityQueue<IntSet>();
		for (int i = 0; i < sets.length; i++) {
			TIntList temp = new TIntArrayList();
			for (int j = 0; j < sets[i].length; j++) {
				temp.addAll(sets[i][j]);
			}
			temp.sort();
			tempList.add(new IntSet(Integer.toString(i), temp.toArray(), weights[i]));
		}

		for (int i = 0; i < sets.length; i++) {
			IntSet temp = tempList.poll();
			collection.add(temp, i);
		}
		return collection;
	}
}