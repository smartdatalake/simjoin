package eu.smartdatalake.simjoin.fuzzysets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FuzzySetCollection {

	public Map<String, List<Set<String>>> sets;

	public FuzzySetCollection() {
		sets = new HashMap<String, List<Set<String>>>(); 
	}
	
	public FuzzySetCollection(Map<String, List<Set<String>>> sets) {
		this.sets = sets;
	}
	
	public FuzzySetCollection(FuzzySet set) {
		sets = new HashMap<String, List<Set<String>>>();
		sets.put(set.id, set.elements);		
	}

	@Override
	public String toString() {
		String output = "";
		for (String key : sets.keySet()) {
			output += key + ": " + sets.get(key) + "\n";
		}
		return output;
	}
	
	public FuzzySet getSet(int index_key) {
		String key = (String) sets.keySet().toArray()[index_key];
		return new FuzzySet(key, sets.get(key));
	}
}