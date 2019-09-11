package eu.smartdatalake.simjoin.fuzzysets;

import java.util.HashMap;

import eu.smartdatalake.simjoin.sets.JoinResult;

public class FuzzyJoinResult extends JoinResult {
	public HashMap<String, Double> matches;

	public FuzzyJoinResult() {
		matches = new HashMap<String, Double>();
	}
}