package eu.smartdatalake.simjoin.data.prepared;

import gnu.trove.map.TObjectIntMap;

public class PreparedSet {
	TObjectIntMap<String> tokenDictionary;
	
	public TObjectIntMap<String> getDictionary() {
		return tokenDictionary;
	}
	
	public Object getCollection() {
		return null;
	}
	
	public Object getIndex() {
		return null;
	}
}
