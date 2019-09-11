package eu.smartdatalake.simjoin.fuzzysets.alg;


import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class IndexConstructor {

	/** Constructs an inverted index from a given collection */

	public TIntObjectMap<TIntList>[] buildSetInvertedIndex(int[][][] collection, int numTokens) {

		// initialize the index
		@SuppressWarnings("unchecked")
		TIntObjectMap<TIntList>[] idx = new TIntObjectHashMap[numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntObjectHashMap<TIntList>();
		}

		// populate the index
		TIntList invList;
		int token;
		for (int i = 0; i < collection.length; i++) {
			for (int j = 0; j < collection[i].length; j++) {
				for (int k = 0; k < collection[i][j].length; k++) {
					token = collection[i][j][k];
					if (idx[token].containsKey(i)) {
						invList = idx[token].get(i);
					} else {
						invList = new TIntArrayList();
					}
					invList.add(j);
					idx[token].put(i, invList);
				}
			}
		}

		return idx;
	}


}