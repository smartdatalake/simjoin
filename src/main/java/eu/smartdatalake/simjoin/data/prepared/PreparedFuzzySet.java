package eu.smartdatalake.simjoin.data.prepared;

import java.util.ArrayList;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollectionTransformer;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;

public class PreparedFuzzySet extends PreparedSet {
	FuzzyIntSetCollection collection;
	FuzzySetIndex idx;
	
	public PreparedFuzzySet(GroupCollection<ArrayList<String>> data) {
		this.tokenDictionary = FuzzySetCollectionTransformer.constructTokenDictionary(data);
		this.collection = FuzzySetCollectionTransformer.transformCollection(data, tokenDictionary);
		idx = new FuzzySetIndex(collection);
	}
	
	public Object getCollection() {
		return collection;
	}
	
	public Object getIndex() {
		return idx;
	}
}
