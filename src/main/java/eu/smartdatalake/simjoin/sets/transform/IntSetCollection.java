package eu.smartdatalake.simjoin.sets.transform;

import java.util.Arrays;

import gnu.trove.map.TIntObjectMap;

public class IntSetCollection {
	public TIntObjectMap<String> idMap;
	public int numTokens;
	public int[][] sets;
	
	@Override
	public String toString() {
		return Arrays.deepToString(sets) + " "+ idMap.toString();
	}
}