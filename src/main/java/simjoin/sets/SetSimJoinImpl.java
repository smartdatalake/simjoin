package simjoin.sets;

import gnu.trove.map.TObjectIntMap;
import simjoin.sets.alg.AllPairs;
import simjoin.sets.alg.IntJoinResult;
import simjoin.sets.transform.CollectionTransformer;
import simjoin.sets.transform.IntSetCollection;

public class SetSimJoinImpl implements ISetSimJoin {

	@Override
	public JoinResult rangeSearch(TokenSet query, TokenSetCollection collection, double simThreshold,
			boolean returnCounts) {
		TokenSetCollection collection1 = new TokenSetCollection();
		collection1.sets = new TokenSet[] { query };
		return rangeJoin(collection1, collection, simThreshold, returnCounts);
	}

	@Override
	public JoinResult rangeSelfJoin(TokenSetCollection collection, double simThreshold, boolean returnCounts) {
		return rangeJoin(null, collection, simThreshold, returnCounts);
	}

	@Override
	public JoinResult rangeJoin(TokenSetCollection collection1, TokenSetCollection collection2, double simThreshold,
			boolean returnCounts) {

		// Transform the input collections
		long duration = System.nanoTime();
		CollectionTransformer transformer = new CollectionTransformer();
		TObjectIntMap<String> tokenDictionary = transformer.constructTokenDictionary(collection2);
		IntSetCollection transformedCollection2 = transformer.transformCollection(collection2, tokenDictionary);
		IntSetCollection transformedCollection1 = collection1 == null ? transformedCollection2
				: transformer.transformCollection(collection1, tokenDictionary);
		duration = System.nanoTime() - duration;
		System.out.println("Transform time: " + duration / 1000000000.0 + " sec.");

		// Execute the join
		duration = System.nanoTime();
		AllPairs joinAlg = new AllPairs();
		IntJoinResult joinResult = joinAlg.join(transformedCollection1, transformedCollection2, simThreshold,
				returnCounts);
		duration = System.nanoTime() - duration;
		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");

		// Transform back the result
		JoinResult result = transformer.transformResult(joinResult, transformedCollection1.idMap,
				transformedCollection2.idMap);

		return result;
	}
}