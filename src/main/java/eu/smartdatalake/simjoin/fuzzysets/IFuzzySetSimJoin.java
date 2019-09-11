package eu.smartdatalake.simjoin.fuzzysets;

import eu.smartdatalake.simjoin.sets.JoinResult;

public interface IFuzzySetSimJoin {

	public JoinResult rangeSearch(FuzzySet query, FuzzySetCollection collection, double simThreshold,
			boolean returnCounts);

	public JoinResult rangeSelfJoin(FuzzySetCollection collection, double simThreshold, boolean returnCounts);

	public JoinResult rangeJoin(FuzzySetCollection collection1, FuzzySetCollection collection2, double simThreshold,
			boolean returnCounts);
}