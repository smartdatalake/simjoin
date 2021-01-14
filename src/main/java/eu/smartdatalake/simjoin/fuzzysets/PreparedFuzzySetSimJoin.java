package eu.smartdatalake.simjoin.fuzzysets;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.alg.KNNJoin;
import eu.smartdatalake.simjoin.fuzzysets.alg.ThresholdJoin;
import eu.smartdatalake.simjoin.fuzzysets.alg.TopKJoin;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import gnu.trove.map.TObjectIntMap;
import eu.smartdatalake.simjoin.GroupCollection;

public class PreparedFuzzySetSimJoin implements Runnable {

	public final static int TYPE_THRESHOLD = 0;
	public final static int TYPE_KNN = 1;
	private static final Logger logger = LogManager.getLogger(PreparedFuzzySetSimJoin.class);
	public long timeout;

	int type;
	GroupCollection<ArrayList<String>> collection1;
	FuzzyIntSetCollection collection2;
	double threshold;
	double limitThreshold;
	ConcurrentLinkedQueue<MatchingPair> results;

	TObjectIntMap<String> tokenDictionary;
	FuzzySetIndex idx;

	public PreparedFuzzySetSimJoin(int type, GroupCollection<ArrayList<String>> collection1,
			FuzzyIntSetCollection collection2, double threshold, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results, TObjectIntMap<String> tokenDictionary, FuzzySetIndex idx) {
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

	public void thresholdJoin(GroupCollection<ArrayList<String>> collection1,
			FuzzyIntSetCollection collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		FuzzyIntSetCollection transformedCollection1 = preprocess(collection1);

		// Execute the join
		long duration = System.nanoTime();
		ThresholdJoin joinAlg = new ThresholdJoin(timeout, idx);
		joinAlg.join(transformedCollection1, collection2, threshold, results);
		duration = System.nanoTime() - duration;
		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	public void knnJoin(GroupCollection<ArrayList<String>> collection1, FuzzyIntSetCollection collection2,
			int k, double limitThreshold, ConcurrentLinkedQueue<MatchingPair> results) {

		// Preprocess the input collections
		FuzzyIntSetCollection transformedCollection1 = preprocess(collection1);

		// Execute the join
		long duration = System.nanoTime();
		KNNJoin joinAlg = new KNNJoin(timeout, idx);
		joinAlg.join(transformedCollection1, collection2, k, limitThreshold, results);
		duration = System.nanoTime() - duration;

		logger.info("Join time: " + duration / 1000000000.0 + " sec.");
	}

	private FuzzyIntSetCollection preprocess(GroupCollection<ArrayList<String>> collection) {

		long duration = System.nanoTime();
		FuzzyIntSetCollection transformedCollection = FuzzySetCollectionTransformer.transformCollection(collection,
				tokenDictionary);
		duration = System.nanoTime() - duration;

		logger.info("Transform time: " + duration / 1000000000.0 + " sec.");
		logger.info("Collection size: " + transformedCollection.sets.length);

		return transformedCollection;
	}

	@Override
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