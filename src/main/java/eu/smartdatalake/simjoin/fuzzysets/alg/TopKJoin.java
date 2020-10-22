package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyKIntMatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.fuzzysets.util.SignatureEvent;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements top-k fuzzy set similarity join.
 *
 */
public class TopKJoin extends KNNJoin {
	private static final Logger logger = LogManager.getLogger(TopKJoin.class);
	long timeout;
	
	public TopKJoin(long timeout) {
		super(-1);
		this.timeout = timeout;
	}
	
	/**
	 * Implements top-k self-join.
	 * 
	 * @param collection The input collection.
	 * @param k          The number of pairs to return.
	 * @param results    The queue to which the results are added.
	 */
	public void selfJoin(FuzzyIntSetCollection collection, int k, ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, k, results);
	}

	/**
	 * Implements top-k join.
	 * 
	 * @param collection1 The left collection.
	 * @param collection2 The right collection.
	 * @param k           The number of pairs to return.
	 * @param results     The queue to which the results are added.
	 */
	public void join(FuzzyIntSetCollection collection1, FuzzyIntSetCollection collection2, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {

		long totalMatches = 0;
		/* CREATE INDEX */
		indexingTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex(collection2);

		indexingTime = System.nanoTime() - indexingTime;
		logger.info("Indexing Time: " + indexingTime / 1000000000.0 + " sec.");
		boolean self = collection1 == collection2;

		IntSetCollection flattenedTransformedCollection1 = collection1.flatten();
		IntSetCollection flattenedTransformedCollection2 = flattenedTransformedCollection1;
		if (!self)
			flattenedTransformedCollection2 = collection2.flatten();
		logger.info("Flattened both collections");

		ConcurrentLinkedQueue<MatchingPair> results2 = new ConcurrentLinkedQueue<MatchingPair>();
		eu.smartdatalake.simjoin.sets.alg.TopKJoin joinAlg = new eu.smartdatalake.simjoin.sets.alg.TopKJoin(-1);
		if (!self)
			joinAlg.join(flattenedTransformedCollection1, flattenedTransformedCollection2, k, results2);
		else
			joinAlg.selfJoin(flattenedTransformedCollection1, k, results2);
		System.out.println("Standard TopK completed.");

		TreeSet<FuzzyKIntMatchingPair> resultPairs = new TreeSet<FuzzyKIntMatchingPair>();
		TIntSet[] seen = new TIntSet[collection1.sets.length];
		for (int i = 0; i < seen.length; i++) {
			seen[i] = new TIntHashSet();
		}

		while (!results2.isEmpty()) {
			MatchingPair t = results2.poll();
			int leftID = Integer.parseInt(t.leftID);
			int rightID = Integer.parseInt(t.rightID);
			FuzzyKIntMatchingPair cm = new FuzzyKIntMatchingPair(leftID, rightID, collection1.sets[leftID],
					collection2.sets[rightID], collection1.weights[leftID], collection2.weights[rightID]);
			cm.score = cm.weightedRatio * cm.evaluate();
			resultPairs.add(cm);
			seen[leftID].add(rightID);
			if (self)
				seen[rightID].add(leftID);
			if (resultPairs.size() == k)
				break;
			// System.out.println(cm.toString());
		}

		initLoop: while (resultPairs.size() < k) {
			for (int leftSet = 0; leftSet < collection1.sets.length; leftSet++) {
				int[][] R = collection1.sets[leftSet];
				for (int[] r : R) {
					for (int tok : r) {
						if (tok < 0)
							continue;
						for (int rightID : idx.idx[tok].keys()) {
							if (seen[leftSet].contains(rightID) || (self && leftSet == rightID))
								continue;
							FuzzyKIntMatchingPair cm = new FuzzyKIntMatchingPair(leftSet, rightID, R,
									collection2.sets[rightID], collection1.weights[leftSet],
									collection2.weights[rightID]);
							cm.score = cm.weightedRatio * cm.evaluate();
							resultPairs.add(cm);
							seen[leftSet].add(rightID);
							if (self)
								seen[rightID].add(leftSet);
							if (resultPairs.size() == k)
								break initLoop;
						}
					}
				}
			}
		}
		logger.info("Transforming results completed.");

		double simThreshold = resultPairs.last().score;
		logger.info("Initial threshold is " + simThreshold);

		PriorityQueue<FuzzyKIntMatchingPair> candidatePairs = new PriorityQueue<FuzzyKIntMatchingPair>();
		SignatureEvent[] signatureEvents = new SignatureEvent[collection1.sets.length];
		for (int i = 0; i < signatureEvents.length; i++) {
			double weightedThreshold = 2.0 / (collection1.weights[i] + 1) * simThreshold;
			if (weightedThreshold > 1.0)
				continue;
			weightedSets++;

			int[][] R = collection1.sets[i];
			int recordLength = R.length;
			signatureEvents[i] = new SignatureEvent(i, R, idx.costs);
			signatureEvents[i].computeUnflattenedSignature(weightedThreshold);
			signatureEvents[i].computeElementBounds();

			// compute bounds for length filter
			int recMinLength = (int) Math.ceil(recordLength * weightedThreshold);
			int recMaxLength = (int) Math.floor(recordLength / weightedThreshold);

			/* CANDIDATE GENERATION */
			TIntSet cands = new TIntHashSet();
			for (int ri = 0; ri < signatureEvents[i].unflattenedSignature.length; ri++) {
				for (int token : signatureEvents[i].unflattenedSignature[ri].toArray()) {
					if (token < 0)
						continue;
					int startIndex = 0;
					int endIndex = idx.lengths[token].size();
					if (self)
						startIndex = idx.lengths[token].binarySearch(i); // true_min is > 0, since i is in tokenList

					for (int S : idx.lengths[token].toArray(startIndex, endIndex - startIndex)) {
						/* SIZE FILTER */
						if (self && i == S)
							continue;

						if (!self && collection2.sets[S].length < recMinLength)
							continue;

						if (collection2.sets[S].length > recMaxLength)
							break;

						if (seen[i].contains(S))
							continue;

						cands.add(S);

					}
				}
			}

			for (int c : cands.toArray()) {
				candidatePairs.add(new FuzzyKIntMatchingPair(i, c, R, collection2.sets[c], 1, collection1.weights[i],
						collection2.weights[c]));
			}

		}
		logger.info("Produced Candidates.");

		/* EXECUTE THE JOIN ALGORITHM */
		joinTime = System.nanoTime();
		ProgressBar pb = new ProgressBar(k);
		while (totalMatches != k) {
			if (timeout > 0 && joinTime > timeout)
				return;
			if (candidatePairs.isEmpty() || (resultPairs.first().score >= candidatePairs.peek().score)) {
				FuzzyKIntMatchingPair cm = resultPairs.pollFirst();
				if (results != null)
					results.add(
							new MatchingPair(collection1.keys[cm.leftInd], collection2.keys[cm.rightInd], cm.score));
				totalMatches++;
				pb.progress(joinTime);
				continue;
			}

			FuzzyKIntMatchingPair cm = candidatePairs.poll();
			if (cm.stage == 4) {
				seen[cm.leftInd].add(cm.rightInd);
				if (self)
					seen[cm.rightInd].add(cm.leftInd);
				if (cm.score < simThreshold)
					continue;
				resultPairs.add(cm);
				resultPairs.pollLast();
				simThreshold = resultPairs.last().score;
			} else {
				if (signatureEvents[cm.leftInd].threshold != simThreshold && cm.stage == 1) {
					signatureEvents[cm.leftInd].computeUnflattenedSignature(simThreshold);
					signatureEvents[cm.leftInd].computeElementBounds();
				}
				if (1 / cm.weightedRatio * simThreshold > 1.0) {
					seen[cm.leftInd].add(cm.rightInd);
					if (self)
						seen[cm.rightInd].add(cm.leftInd);
				}
				if (!search(cm, simThreshold, idx, signatureEvents[cm.leftInd].elementBounds,
						signatureEvents[cm.leftInd].unflattenedSignature)) {
					seen[cm.leftInd].add(cm.rightInd);
					if (self)
						seen[cm.rightInd].add(cm.leftInd);
				} else {
					candidatePairs.add(cm);
				}
			}

		}

		joinTime = System.nanoTime() - joinTime;

		logger.info("Left Size: " + collection1.sets.length);
		logger.info("Right Size: " + collection2.sets.length);
		logger.info("Total Join Time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("\tTransformation Time: " + transformationTime / 1000000000.0 + " sec.");
		logger.info("\tInitializing Time: " + initializingTime / 1000000000.0 + " sec.");
		logger.info("\tCandidate Generation Time: " + candidateTime / 1000000000.0 + " sec.");
		logger.info("\tSignature Generation Time: " + signatureGenerationTime / 1000000000.0 + " sec.");
		logger.info("\tSearch Time: " + searchTime / 1000000000.0 + " sec.");
		logger.info("\t\tCheck Filter Time: " + checkFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tNN Filter Time: " + nnFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tVerification Time: " + verificationTime / 1000000000.0 + " sec.");
		logger.info("\tIndexing Time: " + indexingTime / 1000000000.0 + " sec.");

		logger.info("Initial Candidates: " + totalCandidates);
		logger.info("Check Filter Candidates: " + totalCheckFilterCandidates);
		logger.info("NN Filter Candidates: " + totalNNFilterCandidates);

		logger.info("Total Matches: " + totalMatches);
		if (results == null)
			System.out.println("Total Matches: " + totalMatches);
	}
}