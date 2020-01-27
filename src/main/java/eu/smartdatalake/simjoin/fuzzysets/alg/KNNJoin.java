package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntMatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.fuzzysets.util.SignatureEvent;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
//import eu.smartdatalake.simjoin.sets.alg.KNNJoin;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import eu.smartdatalake.simjoin.MatchingPair;

/**
 * Implements kNN fuzzy set similarity join.
 *
 */
public class KNNJoin extends ThresholdJoin {
	private static final Logger logger = LogManager.getLogger(KNNJoin.class);
	protected long initializingTime = 0, candidateTime = 0, initializingTime1 = 0, initializingTime11 = 0,
			initializingTime12 = 0, initializingTime2 = 0, initializingTime3 = 0, bucketInitializingTime = 0;

	/**
	 * Implements kNN self-join.
	 * 
	 * @param collection
	 *            The input collection.
	 * @param k
	 *            The number of nearest neighbors to return for each group.
	 * @param results
	 *            The queue to which the results are added.
	 */
	public void selfJoin(FuzzyIntSetCollection collection, int k, ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, k, results);
	}

	/**
	 * Implements kNN join.
	 * 
	 * @param collection1
	 *            The left collection.
	 * @param collection2
	 *            The right collection.
	 * @param k
	 *            The number of nearest neighbors to return for each group.
	 * @param results
	 *            The queue to which the results are added.
	 */
	public void join(FuzzyIntSetCollection collection1, FuzzyIntSetCollection collection2, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {

		/* CREATE INDEX */
		long startTime2;
		indexingTime = System.nanoTime();
		boolean self = collection1 == collection2;

		bucketInitializingTime = System.nanoTime();
		IntSetCollection flattenedTransformedCollection1 = collection1.flatten();
		IntSetCollection flattenedTransformedCollection2 = flattenedTransformedCollection1;
		if (!self)
			flattenedTransformedCollection2 = collection2.flatten();
		System.out.println("Flattened both collections");

		ConcurrentLinkedQueue<MatchingPair> results2 = new ConcurrentLinkedQueue<MatchingPair>();
		eu.smartdatalake.simjoin.sets.alg.KNNJoin joinAlg = new eu.smartdatalake.simjoin.sets.alg.KNNJoin();
		joinAlg.join(flattenedTransformedCollection1, flattenedTransformedCollection2, k, results2);
		System.out.println("Standard KNN completed.");

		TIntSet[] knnResults = new TIntSet[collection1.sets.length];
		for (int i = 0; i < knnResults.length; i++) {
			knnResults[i] = new TIntHashSet();
		}
		while (!results2.isEmpty()) {
			MatchingPair mp = results2.poll();
			knnResults[Integer.parseInt(mp.leftID)].add(Integer.parseInt(mp.rightID));
		}
		System.out.println("Transforming results completed.");
		bucketInitializingTime = System.nanoTime() - bucketInitializingTime;

		FuzzySetIndex idx = new FuzzySetIndex(collection2);
		indexingTime = System.nanoTime() - indexingTime;
		System.out.println("Indexing Time: " + indexingTime / 1000000000.0 + " sec.");

		/* EXECUTE THE JOIN ALGORITHM */
		joinTime = System.nanoTime();

		ProgressBar pb = new ProgressBar(collection1.sets.length);
		for (int i = 0; i < collection1.sets.length; i++) {
			// progress bar
			pb.progress(joinTime);

			int[][] R = collection1.sets[i];
			int recordLength = R.length;

			startTime2 = System.nanoTime();
			long startTime3 = System.nanoTime();
			TreeSet<FuzzyIntMatchingPair> resultPairs = new TreeSet<FuzzyIntMatchingPair>();
			TIntSet seen = new TIntHashSet();
			for (int setID : knnResults[i].toArray()) {
				long startTime4 = System.nanoTime();
				initializingTime11 += System.nanoTime() - startTime4;
				startTime4 = System.nanoTime();
				FuzzyIntMatchingPair cm = new FuzzyIntMatchingPair(i, setID, R, collection2.sets[setID]);
				cm.evaluate();
				resultPairs.add(cm);
				seen.add(cm.rightInd);
				initializingTime12 += System.nanoTime() - startTime4;
			}

			initializingTime1 += System.nanoTime() - startTime3;
			startTime3 = System.nanoTime();

			SignatureEvent querySet = new SignatureEvent(i, R, idx.costs);

			PriorityQueue<FuzzyIntMatchingPair> candidatePairs = new PriorityQueue<FuzzyIntMatchingPair>();
			double simThreshold = resultPairs.last().score;

			initializingTime2 += System.nanoTime() - startTime3;
			startTime3 = System.nanoTime();
			querySet.computeUnflattenedSignature(simThreshold);
			querySet.computeElementBounds();
			long localTotalMatches = 0;
			initializingTime3 += System.nanoTime() - startTime3;
			initializingTime += System.nanoTime() - startTime2;

			startTime2 = System.nanoTime();
			// compute bounds for length filter
			int recMinLength = (int) Math.ceil(recordLength * simThreshold);
			int recMaxLength = (int) Math.floor(recordLength / simThreshold);

			/* CANDIDATE GENERATION */
			TIntObjectMap<TIntObjectMap<TIntSet>> cands = new TIntObjectHashMap<TIntObjectMap<TIntSet>>();
			TIntSet elem_map;
			TIntObjectMap<TIntSet> set_map;
			for (int ri = 0; ri < querySet.unflattenedSignature.length; ri++) {
				int elemMinLength = (int) Math.ceil(R[ri].length * querySet.elementBounds[ri]);
				int elemMaxLength = (int) Math.floor(R[ri].length / querySet.elementBounds[ri]);
				for (int token : querySet.unflattenedSignature[ri].toArray()) {
					for (int S : idx.idx[token].keys()) {
						int rightSetLength = collection2.sets[S].length;
						/* SIZE FILTER */

						if (rightSetLength < recMinLength || rightSetLength > recMaxLength) {
							continue;
						}

						if (seen.contains(S)) {
							continue;
						}

						if (self && i >= S)
							continue;

						if (!cands.containsKey(S)) {
							set_map = new TIntObjectHashMap<TIntSet>();
						} else {
							set_map = cands.get(S);
						}

						if (!set_map.containsKey(ri)) {
							elem_map = new TIntHashSet();
						} else {
							elem_map = set_map.get(ri);
						}

						/* ELEMENT SIZE FILTER */
						for (int sj : idx.idx[token].get(S).toArray()) {
							if (collection2.sets[S][sj].length <= elemMaxLength
									&& collection2.sets[S][sj].length >= elemMinLength)
								elem_map.add(sj);
						}
						if (!elem_map.isEmpty()) {
							set_map.put(ri, elem_map);
							cands.put(S, set_map);
						}
					}
				}
			}

			for (int c : cands.keys()) {
				candidatePairs.add(new FuzzyIntMatchingPair(i, c, R, collection2.sets[c], cands.get(c)));
			}
			candidateTime += System.nanoTime() - startTime2;

			startTime2 = System.nanoTime();
			while (localTotalMatches != k) {
				if (candidatePairs.isEmpty() || (resultPairs.first().score >= candidatePairs.peek().score)) {
					FuzzyIntMatchingPair cm = resultPairs.pollFirst();
					results.add(
							new MatchingPair(collection1.keys[cm.leftInd], collection2.keys[cm.rightInd], cm.score));
					localTotalMatches++;
					continue;
				}

				FuzzyIntMatchingPair cm = candidatePairs.poll();
				if (cm.stage == 4) {
					seen.add(cm.rightInd);
					if (cm.score < simThreshold)
						continue;
					resultPairs.add(cm);
					resultPairs.pollLast();
					simThreshold = resultPairs.last().score;
					querySet.computeUnflattenedSignature(simThreshold);
					querySet.computeElementBounds();
				} else {
					if (!search(cm, simThreshold, idx, querySet.elementBounds)) {
						seen.add(cm.rightInd);
					} else {
						candidatePairs.add(cm);
					}
				}
			}
			searchTime += System.nanoTime() - startTime2;
			totalMatches += localTotalMatches;
		}
		joinTime = System.nanoTime() - joinTime;
		bucketInitializingTime += initializingTime1;

		logger.info("Left Size: " + collection1.sets.length);
		logger.info("Right Size: " + collection2.sets.length);
		logger.info("Total Join Time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("\tTransformation Time: " + transformationTime / 1000000000.0 + " sec.");
		logger.info("\tInitializing Time: " + initializingTime / 1000000000.0 + " sec.");
		logger.info("\t\tInitializing Time1: " + initializingTime1 / 1000000000.0 + " sec.");
		logger.info("\t\tInitializing Time2: " + initializingTime2 / 1000000000.0 + " sec.");
		logger.info("\t\tInitializing Time3: " + initializingTime3 / 1000000000.0 + " sec.");
		logger.info("\tCandidate Generation Time: " + candidateTime / 1000000000.0 + " sec.");
		logger.info("\tSignature Generation Time: " + signatureGenerationTime / 1000000000.0 + " sec.");
		logger.info("\tSearch Time: " + searchTime / 1000000000.0 + " sec.");
		logger.info("\t\tCheck Filter Time: " + checkFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tNN Filter Time: " + nnFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tVerification Time: " + verificationTime / 1000000000.0 + " sec.");
		logger.info("\tIndexing Time: " + indexingTime / 1000000000.0 + " sec.");
		logger.info("\tBucket Initializing Time: " + bucketInitializingTime / 1000000000.0 + " sec.");

		logger.info("Initial Candidates: " + totalCandidates);
		logger.info("Check Filter Elements: " + totalElements);
		logger.info("Check Filter Candidates: " + totalCheckFilterCandidates);
		logger.info("NN Filter Candidates: " + totalNNFilterCandidates);

		logger.info("Total Matches: " + totalMatches);
	}

	@Override
	protected boolean search(FuzzyIntMatchingPair cm, double simThreshold, FuzzySetIndex idx, double[] elementBounds) {

		int stage = cm.stage;

		switch (stage) {
		case 1:
			/* CHECK FILTER */
			startTime = System.nanoTime();
			boolean pass = applyCheckFilter(cm, simThreshold, elementBounds);
			checkFilterTime += System.nanoTime() - startTime;
			cm.stage++; // stage=2
			if (!pass) {
				return false;
			}
			cm.score = cm.calculateUpperBound();
			totalCheckFilterCandidates += 1;
			break;

		case 2:
			/* NEAREST NEIGHBOR FILTER */
			startTime = System.nanoTime();
			cm.score = applyNNFilter(cm, simThreshold, idx);
			cm.stage++; // stage=3
			nnFilterTime += System.nanoTime() - startTime;
			if ((cm.score < simThreshold * cm.leftSet.length)
					|| (cm.score / (cm.leftSet.length + cm.rightSet.length - cm.score) < simThreshold)) {
				return false;
			}
			cm.score = cm.score / (cm.leftSet.length + cm.rightSet.length - cm.score);
			totalNNFilterCandidates += 1;
			break;

		case 3:
			/* VERIFICATION */
			startTime = System.nanoTime();
			cm.score = cm.evaluate();
			verificationTime += System.nanoTime() - startTime;
			cm.stage++; // stage=4
			if (cm.score < simThreshold)
				return false;
		}

		return true;
	}
}