package simjoin.sets;

public interface ISetSimJoin {

	public JoinResult rangeSearch(TokenSet query, TokenSetCollection collection, double simThreshold,
			boolean returnCounts);

	public JoinResult rangeSelfJoin(TokenSetCollection collection, double simThreshold, boolean returnCounts);

	public JoinResult rangeJoin(TokenSetCollection collection1, TokenSetCollection collection2, double simThreshold,
			boolean returnCounts);
}