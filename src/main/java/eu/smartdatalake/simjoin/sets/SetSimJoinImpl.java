package eu.smartdatalake.simjoin.sets;

import eu.smartdatalake.simjoin.sets.alg.AllPairs;
import eu.smartdatalake.simjoin.sets.alg.AllPairsKNN;
import eu.smartdatalake.simjoin.sets.alg.ClosestPairs;
import eu.smartdatalake.simjoin.sets.alg.IntJoinResult;
import eu.smartdatalake.simjoin.sets.alg.SelfClosestPairs;
import eu.smartdatalake.simjoin.sets.transform.CollectionTransformer;
import eu.smartdatalake.simjoin.sets.transform.IntSetCollection;
import eu.smartdatalake.simjoin.sets.transform.CollectionTransformer.TokenFrequencyPair;
import gnu.trove.map.TObjectIntMap;

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

	@Override
	public JoinResult knnSearch(TokenSet query, TokenSetCollection collection, int k) {
		TokenSetCollection collection1 = new TokenSetCollection();
		collection1.sets = new TokenSet[] { query };
		return knnJoin(collection1, collection, k);
	}

	@Override
	public JoinResult knnSelfJoin(TokenSetCollection collection, int k) {
		return knnJoin(null, collection, k);
	}

	@Override
	public JoinResult knnJoin(TokenSetCollection collection1, TokenSetCollection collection2, int k) {

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
		AllPairsKNN joinAlg = new AllPairsKNN();
		IntJoinResult joinResult = joinAlg.join(transformedCollection1, transformedCollection2, k);
		duration = System.nanoTime() - duration;
		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");

		// Transform back the result
		JoinResult result = transformer.transformResult(joinResult, transformedCollection1.idMap,
				transformedCollection2.idMap);

		return result;
	}

	@Override
	public JoinResult closestPairsSelfJoin(TokenSetCollection collection, int k) {
		
		// Transform the input collections
		long duration = System.nanoTime();
		CollectionTransformer transformer = new CollectionTransformer();
		TokenFrequencyPair[] tokenFrequencies = transformer.calculateTokenFrequency(collection);
		TObjectIntMap<String> tokenDictionary = transformer.constructTokenDictionary(collection);
		IntSetCollection transformedCollection = transformer.transformCollection(collection, tokenDictionary);
		duration = System.nanoTime() - duration;
		System.out.println("Transform time: " + duration / 1000000000.0 + " sec.");

		// Execute the join
		duration = System.nanoTime();
		SelfClosestPairs joinAlg = new SelfClosestPairs();
		IntJoinResult joinResult = joinAlg.join(transformedCollection, tokenFrequencies, k);
		duration = System.nanoTime() - duration;
		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");

		// Transform back the result
		JoinResult result = transformer.transformResult(joinResult, transformedCollection.idMap, transformedCollection.idMap);
		
		return result;
	}

	@Override
	public JoinResult closestPairsJoin(TokenSetCollection collection1, TokenSetCollection collection2, int k) {

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
		ClosestPairs joinAlg = new ClosestPairs();
		IntJoinResult joinResult = joinAlg.join(transformedCollection1, transformedCollection2, k);
		duration = System.nanoTime() - duration;
		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");

		// Transform back the result
		JoinResult result = transformer.transformResult(joinResult, transformedCollection1.idMap,
				transformedCollection2.idMap);

		return result;
	}
}