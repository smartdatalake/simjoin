package eu.smartdatalake.simjoin.sets;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.sets.alg.KNNJoin;
import eu.smartdatalake.simjoin.sets.alg.ThresholdJoin;
import eu.smartdatalake.simjoin.sets.alg.TopKJoin;
import gnu.trove.map.TObjectIntMap;
import eu.smartdatalake.simjoin.GroupCollection;

public class PreparedSetSimJoin implements Runnable {

	public final static int TYPE_THRESHOLD = 0;
	public final static int TYPE_KNN = 1;
	private static final Logger logger = LogManager.getLogger(PreparedSetSimJoin.class);
	public long timeout;

	int type;
	GroupCollection<String> collection1;
	IntSetCollection collection2;
	double threshold;
	double limitThreshold;
	ConcurrentLinkedQueue<MatchingPair> results;

	TObjectIntMap<String> tokenDictionary;
	int[][] idx;

	public PreparedSetSimJoin(int type, GroupCollection<String> collection1, IntSetCollection collection2,
			double threshold, double limitThreshold, ConcurrentLinkedQueue<MatchingPair> results,
			TObjectIntMap<String> tokenDictionary, int[][] idx) {
		super();
		this.type = type;
		this.collection1 = collection1;
		this.collection2 = collection2;
		this.threshold = threshold;
		this.limitThreshold = limitThreshold;
		this.results = results;
		this.tokenDictionary = tokenDictionary;
		this.idx = idx;
	}

	public void thresholdJoin(GroupCollection<String> collection1, IntSetCollection collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		IntSetCollection transformedCollection1 = preprocess(collection1);
		
		// Execute the join
		long duration = System.nanoTime();
		ThresholdJoin joinAlg = new ThresholdJoin(timeout, idx);
		joinAlg.join(transformedCollection1, collection2, threshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void knnJoin(GroupCollection<String> collection1, IntSetCollection collection2, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		IntSetCollection transformedCollection1 = preprocess(collection1);
		
		// Execute the join
		long duration = System.nanoTime();
		KNNJoin joinAlg = new KNNJoin(timeout, idx);
		joinAlg.join(transformedCollection1, collection2, k, limitThreshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	private IntSetCollection preprocess(GroupCollection<String> collection) {

		long duration = System.nanoTime();
		IntSetCollection transformedCollection = TokenSetCollectionTransformer.transformCollection(collection,
				tokenDictionary);
		duration = System.nanoTime() - duration;

		logger.info("Transform time: " + duration / 1000000000.0 + " sec.");
		logger.info("Collection size: " + transformedCollection.sets.length);

		return transformedCollection;
	}

	public void run() {
		switch (type) {
		case TYPE_THRESHOLD:
			thresholdJoin(collection1, collection2, threshold, results);
			break;
		case TYPE_KNN:
			knnJoin(collection1, collection2, (int) threshold, limitThreshold, results);
			break;
		default:
			break;
		}
		logger.info("Parameter: " + threshold);
	}
}