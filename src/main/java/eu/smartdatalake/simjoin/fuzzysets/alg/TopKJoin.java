package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntMatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.fuzzysets.util.SignatureEvent;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements top-k fuzzy set similarity join.
 *
 */
public class TopKJoin extends KNNJoin {
	private static final Logger logger = LogManager.getLogger(TopKJoin.class);

	/**
	 * Implements top-k self-join.
	 * 
	 * @param collection
	 *            The input collection.
	 * @param k
	 *            The number of pairs to return.
	 * @param results
	 *            The queue to which the results are added.
	 */
	@Override
	public void selfJoin(FuzzyIntSetCollection collection, int k, ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, k, results);
	}

	/**
	 * Implements top-k join.
	 * 
	 * @param collection1
	 *            The left collection.
	 * @param collection2
	 *            The right collection.
	 * @param k
	 *            The number of pairs to return.
	 * @param results
	 *            The queue to which the results are added.
	 */
	@Override
	public void join(FuzzyIntSetCollection collection1, FuzzyIntSetCollection collection2, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {

		long totalMatches = 0;
		/* CREATE INDEX */
		indexingTime = System.nanoTime();
		boolean self = collection1 == collection2;

		bucketInitializingTime = System.nanoTime();
		IntSetCollection flattenedTransformedCollection1 = collection1.flatten();
		IntSetCollection flattenedTransformedCollection2 = flattenedTransformedCollection1;
		if (!self)
			flattenedTransformedCollection2 = collection2.flatten();
		System.out.println("Flattened both collections");

		ConcurrentLinkedQueue<MatchingPair> results2 = new ConcurrentLinkedQueue<MatchingPair>();
		eu.smartdatalake.simjoin.sets.alg.TopKJoin joinAlg = new eu.smartdatalake.simjoin.sets.alg.TopKJoin();
		if (!self)
			joinAlg.join(flattenedTransformedCollection1, flattenedTransformedCollection2, k, results2);
		else
			joinAlg.selfJoin(flattenedTransformedCollection1, k, results2);
		System.out.println("Standard TopK completed.");

		TreeSet<FuzzyIntMatchingPair> resultPairs = new TreeSet<FuzzyIntMatchingPair>();
		TIntSet[] seen = new TIntSet[collection1.sets.length];
		for (int i = 0; i < seen.length; i++) {
			seen[i] = new TIntHashSet();
		}

		while (!results2.isEmpty()) {
			MatchingPair t = results2.poll();
			int leftID = Integer.parseInt(t.leftID);
			int rightID = Integer.parseInt(t.rightID);
			FuzzyIntMatchingPair cm = new FuzzyIntMatchingPair(leftID, rightID, collection2.sets[leftID],
					collection2.sets[rightID]);
			cm.evaluate();
			resultPairs.add(cm);
			seen[leftID].add(rightID);
			seen[rightID].add(leftID);
			// System.out.println(cm.toString());
		}

		double simThreshold = resultPairs.last().score;

		System.out.println("Transforming results completed.");
		bucketInitializingTime = System.nanoTime() - bucketInitializingTime;

		FuzzySetIndex idx = new FuzzySetIndex(collection2);

		indexingTime = System.nanoTime() - indexingTime;
		System.out.println("Indexing Time: " + indexingTime / 1000000000.0 + " sec.");

		PriorityQueue<FuzzyIntMatchingPair> candidatePairs = new PriorityQueue<FuzzyIntMatchingPair>();
		SignatureEvent[] signatureEvents = new SignatureEvent[collection1.sets.length];
		for (int i = 0; i < signatureEvents.length; i++) {
			int[][] R = collection1.sets[i];
			int recordLength = R.length;
			signatureEvents[i] = new SignatureEvent(i, R, idx.costs);
			signatureEvents[i].computeUnflattenedSignature(simThreshold);
			signatureEvents[i].computeElementBounds();

			// compute bounds for length filter
			int recMinLength = (int) Math.ceil(recordLength * simThreshold);
			int recMaxLength = (int) Math.floor(recordLength / simThreshold);

			/* CANDIDATE GENERATION */
			TIntObjectMap<TIntObjectMap<TIntSet>> cands = new TIntObjectHashMap<TIntObjectMap<TIntSet>>();
			TIntSet elem_map;
			TIntObjectMap<TIntSet> set_map;
			for (int ri = 0; ri < signatureEvents[i].unflattenedSignature.length; ri++) {
				int elemMinLength = (int) Math.ceil(R[ri].length * signatureEvents[i].elementBounds[ri]);
				int elemMaxLength = (int) Math.floor(R[ri].length / signatureEvents[i].elementBounds[ri]);
				for (int token : signatureEvents[i].unflattenedSignature[ri].toArray()) {
					for (int S : idx.idx[token].keys()) {
						int rightSetLength = collection2.sets[S].length;
						/* SIZE FILTER */

						if (rightSetLength < recMinLength || rightSetLength > recMaxLength) {
							continue;
						}

						if (seen[i].contains(S)) {
							continue;
						}

						if (self & i >= S)
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
		}

		/* EXECUTE THE JOIN ALGORITHM */
		joinTime = System.nanoTime();
		ProgressBar pb = new ProgressBar(k);
		while (totalMatches != k) {
			// System.out.println(totalMatches + " " + resultPairs.size() + " "
			// + candidatePairs.size()+"\t"+resultPairs.first().score+"
			// "+candidatePairs.peek().score+" "+resultPairs.last().score);
			if (candidatePairs.isEmpty() || (resultPairs.first().score >= candidatePairs.peek().score)) {
				FuzzyIntMatchingPair cm = resultPairs.pollFirst();
				results.add(new MatchingPair(collection1.keys[cm.leftInd], collection2.keys[cm.rightInd], cm.score));
				totalMatches++;
				pb.progress(joinTime);
				continue;
			}

			FuzzyIntMatchingPair cm = candidatePairs.poll();
			if (cm.stage == 4) {
				seen[cm.leftInd].add(cm.rightInd);
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
				if (!search(cm, simThreshold, idx, signatureEvents[cm.leftInd].elementBounds)) {
					seen[cm.leftInd].add(cm.rightInd);
					seen[cm.rightInd].add(cm.leftInd);
				} else {
					candidatePairs.add(cm);
				}
			}

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
}