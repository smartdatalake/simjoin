package eu.smartdatalake.simjoin.fuzzysets.transform;

import java.util.Set;

public class FuzzyIntSetCollection {

	public int[][][] sets;
	public Set<String> keys;

	public FuzzyIntSetCollection(int[][][] sets, Set<String> keys) {
		this.sets = sets;
		this.keys = keys;
	}
}