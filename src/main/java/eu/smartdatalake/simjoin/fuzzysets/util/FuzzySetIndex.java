package eu.smartdatalake.simjoin.fuzzysets.util;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Inverted index for fuzzy sets.
 *
 */
public class FuzzySetIndex {

	public TIntObjectMap<TIntSet>[] idx;
	public int[] costs;

	@SuppressWarnings("unchecked")
	public FuzzySetIndex(FuzzyIntSetCollection collection) {

		int numTokens = collection.numTokens;
		int[][][] sets = collection.sets;

		// initialize the index
		idx = new TIntObjectHashMap[numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntObjectHashMap<TIntSet>();
		}
		costs = new int[idx.length];

		// populate the index
		TIntSet invList;
		int token;
		for (int i = 0; i < sets.length; i++) {
			for (int j = 0; j < sets[i].length; j++) {
				for (int k = 0; k < sets[i][j].length; k++) {
					token = sets[i][j][k];
					if (idx[token].containsKey(i)) {
						invList = idx[token].get(i);
					} else {
						invList = new TIntHashSet();
					}
					invList.add(j);
					idx[token].put(i, invList);
				}
			}
		}

		for (int tok = 0; tok < idx.length; tok++) {
			int total = 0;
			for (int S : idx[tok].keys()) {
				total += idx[tok].get(S).size();
			}
			costs[tok] = total;
		}
	}
}