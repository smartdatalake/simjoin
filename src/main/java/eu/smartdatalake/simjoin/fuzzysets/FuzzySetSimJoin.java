package eu.smartdatalake.simjoin.fuzzysets;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import eu.smartdatalake.simjoin.ISimJoin;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.alg.KNNJoin;
import eu.smartdatalake.simjoin.fuzzysets.alg.ThresholdJoin;
import eu.smartdatalake.simjoin.fuzzysets.alg.TopKJoin;
import gnu.trove.map.TObjectIntMap;
import eu.smartdatalake.simjoin.GroupCollection;

public class FuzzySetSimJoin implements ISimJoin<ArrayList<String>>, Runnable {

	public final static int TYPE_THRESHOLD = 0;
	public final static int TYPE_KNN = 1;
	public final static int TYPE_TOPK = 2;

	int type;
	GroupCollection<ArrayList<String>> collection1;
	GroupCollection<ArrayList<String>> collection2;
	double threshold;
	ConcurrentLinkedQueue<MatchingPair> results;

	public FuzzySetSimJoin(int type, GroupCollection<ArrayList<String>> collection, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {
		super();
		this.type = type;
		this.collection1 = collection;
		this.collection2 = null;
		this.threshold = threshold;
		this.results = results;
	}

	public FuzzySetSimJoin(int type, GroupCollection<ArrayList<String>> collection1,
			GroupCollection<ArrayList<String>> collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {
		super();
		this.type = type;
		this.collection1 = collection1;
		this.collection2 = collection2;
		this.threshold = threshold;
		this.results = results;
	}

	public FuzzySetSimJoin() {
		super();
	}

	public void thresholdJoin(GroupCollection<ArrayList<String>> collection, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collection
		FuzzyIntSetCollection transformedCollection = preprocess(collection);

		// Execute the join
		long duration = System.nanoTime();
		ThresholdJoin joinAlg = new ThresholdJoin();
		joinAlg.selfJoin(transformedCollection, threshold, results);
		duration = System.nanoTime() - duration;

		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void thresholdJoin(GroupCollection<ArrayList<String>> collection1,
			GroupCollection<ArrayList<String>> collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		FuzzyIntSetCollection[] transformedCollections = preprocess(collection1, collection2);
		FuzzyIntSetCollection transformedCollection1 = transformedCollections[0];
		FuzzyIntSetCollection transformedCollection2 = transformedCollections[1];

		// Execute the join
		long duration = System.nanoTime();
		ThresholdJoin joinAlg = new ThresholdJoin();
		joinAlg.join(transformedCollection1, transformedCollection2, threshold, results);
		duration = System.nanoTime() - duration;

		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void knnJoin(GroupCollection<ArrayList<String>> collection, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collection
		FuzzyIntSetCollection transformedCollection = preprocess(collection);

		// Execute the join
		long duration = System.nanoTime();
		KNNJoin joinAlg = new KNNJoin();
		joinAlg.selfJoin(transformedCollection, k, results);
		duration = System.nanoTime() - duration;

		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void knnJoin(GroupCollection<ArrayList<String>> collection1, GroupCollection<ArrayList<String>> collection2,
			int k, ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		FuzzyIntSetCollection[] transformedCollections = preprocess(collection1, collection2);
		FuzzyIntSetCollection transformedCollection1 = transformedCollections[0];
		FuzzyIntSetCollection transformedCollection2 = transformedCollections[1];

		// Execute the join
		long duration = System.nanoTime();
		KNNJoin joinAlg = new KNNJoin();
		joinAlg.join(transformedCollection1, transformedCollection2, k, results);
		duration = System.nanoTime() - duration;

		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void topkJoin(GroupCollection<ArrayList<String>> collection, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collection
		FuzzyIntSetCollection transformedCollection = preprocess(collection);

		// Execute the join
		long duration = System.nanoTime();
		TopKJoin joinAlg = new TopKJoin();
		joinAlg.selfJoin(transformedCollection, k, results);
		duration = System.nanoTime() - duration;

		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void topkJoin(GroupCollection<ArrayList<String>> collection1, GroupCollection<ArrayList<String>> collection2,
			int k, ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		FuzzyIntSetCollection[] transformedCollections = preprocess(collection1, collection2);
		FuzzyIntSetCollection transformedCollection1 = transformedCollections[0];
		FuzzyIntSetCollection transformedCollection2 = transformedCollections[1];

		// Execute the join
		long duration = System.nanoTime();
		TopKJoin joinAlg = new TopKJoin();
		joinAlg.join(transformedCollection1, transformedCollection2, k, results);
		duration = System.nanoTime() - duration;

		System.out.println("Join time: " + duration / 1000000000.0 + " sec.");
	}

	private FuzzyIntSetCollection preprocess(GroupCollection<ArrayList<String>> collection) {

		long duration = System.nanoTime();
		FuzzySetCollectionTransformer transformer = new FuzzySetCollectionTransformer();
		TObjectIntMap<String> tokenDictionary = transformer.constructTokenDictionary(collection);
		FuzzyIntSetCollection transformedCollection = transformer.transformCollection(collection, tokenDictionary);
		duration = System.nanoTime() - duration;

		System.out.println("Transform time: " + duration / 1000000000.0 + " sec.");
		System.out.println("Collection size: " + transformedCollection.sets.length);

		return transformedCollection;
	}

	private FuzzyIntSetCollection[] preprocess(GroupCollection<ArrayList<String>> collection1,
			GroupCollection<ArrayList<String>> collection2) {

		long duration = System.nanoTime();
		FuzzySetCollectionTransformer transformer = new FuzzySetCollectionTransformer();
		TObjectIntMap<String> tokenDictionary = transformer.constructTokenDictionary(collection2);
		FuzzyIntSetCollection transformedCollection2 = transformer.transformCollection(collection2, tokenDictionary);
		FuzzyIntSetCollection transformedCollection1 = transformer.transformCollection(collection1, tokenDictionary);
		duration = System.nanoTime() - duration;

		System.out.println("Transform time: " + duration / 1000000000.0 + " sec.");
		System.out.println("Left collection size: " + transformedCollection1.sets.length);
		System.out.println("Right collection size: " + transformedCollection2.sets.length);

		return new FuzzyIntSetCollection[] { transformedCollection1, transformedCollection2 };
	}

	@Override
	public void run() {
		switch (type) {
		case TYPE_THRESHOLD:
			if (collection2 == null) {
				thresholdJoin(collection1, threshold, results);
			} else {
				thresholdJoin(collection1, collection2, threshold, results);
			}
			break;
		case TYPE_KNN:
			if (collection2 == null) {
				knnJoin(collection1, (int) threshold, results);
			} else {
				knnJoin(collection1, collection2, (int) threshold, results);
			}
			break;
		case TYPE_TOPK:
			if (collection2 == null) {
				topkJoin(collection1, (int) threshold, results);
			} else {
				topkJoin(collection1, collection2, (int) threshold, results);
			}
			break;
		default:
			break;
		}
	}
}