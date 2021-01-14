package eu.smartdatalake.simjoin.data.prepared;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
import eu.smartdatalake.simjoin.sets.TokenSetCollectionTransformer;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

public class PreparedStandardSet extends PreparedSet {
	IntSetCollection collection;
	int[][] idx;

	public PreparedStandardSet(GroupCollection<String> data, double threshold) {
		this.tokenDictionary = TokenSetCollectionTransformer.constructTokenDictionary(data);
		this.collection = TokenSetCollectionTransformer.transformCollection(data, tokenDictionary);

		if (threshold == 0.0) {
			TIntList[] tmpIdx = new TIntList[collection.numTokens];
			for (int i = 0; i < tmpIdx.length; i++) {
				tmpIdx[i] = new TIntArrayList();
			}

			// Index construction
			for (int i = 0; i < collection.sets.length; i++) {
				// Since no threshold is known beforehand, index is constructed with
				// all tokens (not prefixes)
				for (int j = 0; j < collection.sets[i].length; j++) {
					tmpIdx[collection.sets[i][j]].add(i);
				}
			}
			// convert index to int[][]
			idx = new int[collection.numTokens][];
			for (int i = 0; i < idx.length; i++) {
				idx[i] = tmpIdx[i].toArray();
			}
		} else {
			// Index construction
			// create an empty inverted list for each token
			TIntList[] tmpIdx = new TIntList[collection.numTokens];
			for (int i = 0; i < tmpIdx.length; i++) {
				tmpIdx[i] = new TIntArrayList();
			}
			// iterate over the sets and populate the inverted lists
			for (int i = 0; i < collection.sets.length; i++) {
				double weightedThresholdr = 2.0 / (collection.weights[i] + 1) * threshold;
				int prefixLength = collection.sets[i].length
						- (int) Math.ceil(collection.sets[i].length * weightedThresholdr) + 1;

				for (int j = 0; j < prefixLength; j++) {
					tmpIdx[collection.sets[i][j]].add(i);
				}
			}
			// convert index to int[][]
			idx = new int[collection.numTokens][];
			for (int i = 0; i < idx.length; i++) {
				idx[i] = tmpIdx[i].toArray();
			}
		}
	}
	
	public Object getCollection() {
		return collection;
	}
	
	public Object getIndex() {
		return idx;
	}
}
