package eu.smartdatalake.simjoin.sets;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.ISimJoin;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.sets.alg.KNNJoin;
import eu.smartdatalake.simjoin.sets.alg.ThresholdJoin;
import eu.smartdatalake.simjoin.sets.alg.TopKJoin;
import gnu.trove.map.TObjectIntMap;
import eu.smartdatalake.simjoin.GroupCollection;

public class SetSimJoin implements ISimJoin<String>, Runnable {

	public final static int TYPE_THRESHOLD = 0;
	public final static int TYPE_KNN = 1;
	public final static int TYPE_TOPK = 2;
	private static final Logger logger = LogManager.getLogger(SetSimJoin.class);
	public long timeout;

	int type;
	GroupCollection<String> collection1;
	GroupCollection<String> collection2;
	double threshold;
	double limitThreshold;
	ConcurrentLinkedQueue<MatchingPair> results;

	public SetSimJoin(int type, GroupCollection<String> collection1, GroupCollection<String> collection2,
			double threshold, double limitThreshold, ConcurrentLinkedQueue<MatchingPair> results) {
		super();
		this.type = type;
		this.collection1 = collection1;
		this.collection2 = collection2;
		this.threshold = threshold;
		this.limitThreshold = limitThreshold;
		this.results = results;
	}

	public void thresholdJoin(GroupCollection<String> collection, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collection
		IntSetCollection transformedCollection = preprocess(collection);

		// Execute the join
		long duration = System.nanoTime();
		ThresholdJoin joinAlg = new ThresholdJoin(timeout);
		joinAlg.selfJoin(transformedCollection, threshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void thresholdJoin(GroupCollection<String> collection1, GroupCollection<String> collection2,
			double threshold, ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		IntSetCollection[] transformedCollections = preprocess(collection1, collection2);
		IntSetCollection transformedCollection1 = transformedCollections[0];
		IntSetCollection transformedCollection2 = transformedCollections[1];

		// Execute the join
		long duration = System.nanoTime();
		ThresholdJoin joinAlg = new ThresholdJoin(timeout);
		joinAlg.join(transformedCollection1, transformedCollection2, threshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void knnJoin(GroupCollection<String> collection, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collection
		IntSetCollection transformedCollection = preprocess(collection);

		// Execute the join
		long duration = System.nanoTime();
		KNNJoin joinAlg = new KNNJoin(timeout);
		joinAlg.selfJoin(transformedCollection, k, limitThreshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void knnJoin(GroupCollection<String> collection1, GroupCollection<String> collection2, int k,
			double limitThreshold, ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		IntSetCollection[] transformedCollections = preprocess(collection1, collection2);
		IntSetCollection transformedCollection1 = transformedCollections[0];
		IntSetCollection transformedCollection2 = transformedCollections[1];

		// Execute the join
		long duration = System.nanoTime();
		KNNJoin joinAlg = new KNNJoin(timeout);
		joinAlg.join(transformedCollection1, transformedCollection2, k, limitThreshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void topkJoin(GroupCollection<String> collection, int k, ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collection
		IntSetCollection transformedCollection = preprocess(collection);

		// Execute the join
		long duration = System.nanoTime();
		TopKJoin joinAlg = new TopKJoin(timeout);
		joinAlg.selfJoin(transformedCollection, k, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void topkJoin(GroupCollection<String> collection1, GroupCollection<String> collection2, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		IntSetCollection[] transformedCollections = preprocess(collection1, collection2);
		IntSetCollection transformedCollection1 = transformedCollections[0];
		IntSetCollection transformedCollection2 = transformedCollections[1];

		// Execute the join
		long duration = System.nanoTime();
		TopKJoin joinAlg = new TopKJoin(timeout);
		joinAlg.join(transformedCollection1, transformedCollection2, k, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	private IntSetCollection preprocess(GroupCollection<String> collection) {

		long duration = System.nanoTime();
		TObjectIntMap<String> tokenDictionary = TokenSetCollectionTransformer.constructTokenDictionary(collection);
		IntSetCollection transformedCollection = TokenSetCollectionTransformer.transformCollection(collection, tokenDictionary);
		duration = System.nanoTime() - duration;

		logger.info("Transform time: " + duration / 1000000000.0 + " sec.");
		logger.info("Collection size: " + transformedCollection.sets.length);

		return transformedCollection;
	}

	private IntSetCollection[] preprocess(GroupCollection<String> collection1, GroupCollection<String> collection2) {

		long duration = System.nanoTime();
		TObjectIntMap<String> tokenDictionary = TokenSetCollectionTransformer.constructTokenDictionary(collection2);
		IntSetCollection transformedCollection2 = TokenSetCollectionTransformer.transformCollection(collection2, tokenDictionary);
		IntSetCollection transformedCollection1 = TokenSetCollectionTransformer.transformCollection(collection1, tokenDictionary);
		duration = System.nanoTime() - duration;

		logger.info("Transform time: " + duration / 1000000000.0 + " sec.");
		logger.info("Left collection size: " + transformedCollection1.sets.length);
		logger.info("Right collection size: " + transformedCollection2.sets.length);

		return new IntSetCollection[] { transformedCollection1, transformedCollection2 };
	}

	public void run() {
		switch (type) {
		case TYPE_THRESHOLD:
			if (collection1 == null) {
				thresholdJoin(collection2, threshold, results);
			} else {
				thresholdJoin(collection1, collection2, threshold, results);
			}
			break;
		case TYPE_KNN:
			if (collection1 == null) {
				knnJoin(collection2, (int) threshold, limitThreshold, results);
			} else {
				knnJoin(collection1, collection2, (int) threshold, limitThreshold, results);
			}
			break;
		case TYPE_TOPK:
			if (collection1 == null) {
				topkJoin(collection2, (int) threshold, results);
			} else {
				topkJoin(collection1, collection2, (int) threshold, results);
			}
			break;
		default:
			break;
		}
		logger.info("Parameter: "+threshold);
	}
}