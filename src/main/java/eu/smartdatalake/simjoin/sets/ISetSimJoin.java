package eu.smartdatalake.simjoin.sets;

public interface ISetSimJoin {

	public JoinResult rangeSearch(TokenSet query, TokenSetCollection collection, double simThreshold,
			boolean returnCounts);

	public JoinResult rangeSelfJoin(TokenSetCollection collection, double simThreshold, boolean returnCounts);

	public JoinResult rangeJoin(TokenSetCollection collection1, TokenSetCollection collection2, double simThreshold,
			boolean returnCounts);

	public JoinResult knnSearch(TokenSet query, TokenSetCollection collection, int k);

	public JoinResult knnSelfJoin(TokenSetCollection collection, int k);

	public JoinResult knnJoin(TokenSetCollection collection1, TokenSetCollection collection2, int k);

	public JoinResult closestPairsSelfJoin(TokenSetCollection collection, int k);

	public JoinResult closestPairsJoin(TokenSetCollection collection1, TokenSetCollection collection2, int k);
}