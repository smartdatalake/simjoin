package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyKIntMatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.fuzzysets.util.SignatureEvent;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
import eu.smartdatalake.simjoin.sets.alg.Verification;
import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import eu.smartdatalake.simjoin.MatchingPair;

/**
 * Implements kNN fuzzy set similarity join.
 *
 */
//public class KNNJoin {
public class KNNJoin extends ThresholdJoin {
	private static final Logger logger = LogManager.getLogger(KNNJoin.class);
	protected long initializingTime = 0, candidateTime = 0;
	private long timeout;

	public KNNJoin(long timeout) {
		super(-1);
		this.timeout = timeout;
	}
	
	/**
	 * Implements kNN self-join.
	 * 
	 * @param collection     The input collection.
	 * @param k              The number of nearest neighbors to return for each
	 *                       group.
	 * @param limitThreshold A threshold for neighbors to avoid low-scores.
	 * @param results        The queue to which the results are added.
	 */
	public void selfJoin(FuzzyIntSetCollection collection, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, k, limitThreshold, results);
	}

	/**
	 * Implements kNN join.
	 * 
	 * @param collection1    The left collection.
	 * @param collection2    The right collection.
	 * @param k              The number of nearest neighbors to return for each
	 *                       group.
	 * @param limitThreshold A threshold for neighbors to avoid low-scores.
	 * @param results        The queue to which the results are added.
	 */
	public void join(FuzzyIntSetCollection collection1, FuzzyIntSetCollection collection2, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		/* CREATE INDEX */
		long startTime2;
		boolean self = collection1 == collection2;

		List<FuzzyKIntMatchingPair>[] cachedPairs = null;
		if (self) {
			cachedPairs = new List[collection2.sets.length];
			for (int i = 0; i < cachedPairs.length; i++)
				cachedPairs[i] = new ArrayList<FuzzyKIntMatchingPair>();
		}

		TIntSet[] knnResults = new TIntSet[collection1.sets.length];
		if (limitThreshold == 0.0) {
			IntSetCollection flattenedTransformedCollection1 = collection1.flatten();
			IntSetCollection flattenedTransformedCollection2 = flattenedTransformedCollection1;
			if (!self)
				flattenedTransformedCollection2 = collection2.flatten();

			ConcurrentLinkedQueue<MatchingPair> results2 = new ConcurrentLinkedQueue<MatchingPair>();
			eu.smartdatalake.simjoin.sets.alg.KNNJoin joinAlg = new eu.smartdatalake.simjoin.sets.alg.KNNJoin(-1);
			joinAlg.join(flattenedTransformedCollection1, flattenedTransformedCollection2, k, limitThreshold, results2);

			for (int i = 0; i < knnResults.length; i++) {
				knnResults[i] = new TIntHashSet();
			}

			while (!results2.isEmpty()) {
				MatchingPair mp = results2.poll();
				knnResults[Integer.parseInt(mp.leftID)].add(Integer.parseInt(mp.rightID));
			}
		}

		indexingTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex(collection2);
		indexingTime = System.nanoTime() - indexingTime;
		System.out.println("Indexing Time: " + indexingTime / 1000000000.0 + " sec.");

		/* EXECUTE THE JOIN ALGORITHM */
		joinTime = System.nanoTime();

		ProgressBar pb = new ProgressBar(collection1.sets.length);
		for (int i = 0; i < collection1.sets.length; i++) {
			if (timeout > 0 && System.nanoTime() - joinTime > timeout)
				return;
			// progress bar
			pb.progress(joinTime);

			int[][] R = collection1.sets[i];
			int recordLength = R.length;
			int local_k = k;

			startTime2 = System.nanoTime();
			TreeSet<FuzzyKIntMatchingPair> resultPairs = new TreeSet<FuzzyKIntMatchingPair>();
			TIntSet seen = new TIntHashSet();

			if (self) {
				for (FuzzyKIntMatchingPair cm : cachedPairs[i]) {
					resultPairs.add(cm);
					if (resultPairs.size() > local_k)
						resultPairs.pollLast();
					seen.add(cm.rightInd);
				}
				cachedPairs[i].clear();
			}

			if (knnResults[i] != null) {
				for (int setID : knnResults[i].toArray()) {
					if (resultPairs.size() == local_k)
						break;
					if (seen.contains(setID))
						continue;
					FuzzyKIntMatchingPair cm = new FuzzyKIntMatchingPair(i, setID, R, collection2.sets[setID],
							collection1.weights[i], collection2.weights[setID]);
					cm.score = cm.weightedRatio * cm.evaluate();
					resultPairs.add(cm);
					seen.add(cm.rightInd);
				}
			} else {
				while (resultPairs.size() != local_k) {
					resultPairs.add(new FuzzyKIntMatchingPair(i, -resultPairs.size(), null, null, 1.0, -1, 0.0));
				}
			}

			if (resultPairs.size() == 0)
				continue;
			local_k = resultPairs.size();

			SignatureEvent querySet = new SignatureEvent(i, R, idx.costs);

			PriorityQueue<FuzzyKIntMatchingPair> candidatePairs = new PriorityQueue<FuzzyKIntMatchingPair>();
			double simThreshold = resultPairs.last().score > limitThreshold ? resultPairs.last().score : limitThreshold;
			double weightedThreshold = 2.0 / (collection1.weights[i] + 1) * simThreshold;
			if (weightedThreshold > 1.0)
				continue;
			weightedSets++;

			querySet.computeUnflattenedSignature(weightedThreshold);
			querySet.computeElementBounds();
			long localTotalMatches = 0;
			initializingTime += System.nanoTime() - startTime2;

			startTime2 = System.nanoTime();
			// compute bounds for length filter
			int recMinLength = (int) Math.ceil(recordLength * weightedThreshold);
			int recMaxLength = (int) Math.floor(recordLength / weightedThreshold);

			/* CANDIDATE GENERATION */
			TIntSet cands = new TIntHashSet();
			for (int ri = 0; ri < querySet.unflattenedSignature.length; ri++) {
				for (int token : querySet.unflattenedSignature[ri].toArray()) {
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

						if (seen.contains(S))
							continue;

						cands.add(S);
						
					}
				}
			}

			for (int c : cands.toArray()) {
				candidatePairs.add(new FuzzyKIntMatchingPair(i, c, R, collection2.sets[c], 1, collection1.weights[i],
						collection2.weights[c]));
			}
			
			totalCandidates+= cands.size();
			candidateTime += System.nanoTime() - startTime2;

			startTime2 = System.nanoTime();
			while (localTotalMatches != local_k) {
				if (candidatePairs.isEmpty() || (simThreshold > candidatePairs.peek().score)) {
					while (!resultPairs.isEmpty()) {
						FuzzyKIntMatchingPair cm = resultPairs.pollFirst();
						if (self && cm.stage == -1 && cm.leftInd < cm.rightInd) // from initBucket && can be used again
																				// later
							cachedPairs[cm.rightInd].add(cm.swap());
						if (cm.score >= simThreshold) {
							if (results != null)
								results.add(new MatchingPair(collection1.keys[cm.leftInd],
										collection2.keys[cm.rightInd], cm.score));
							localTotalMatches++;
						}
					}
					break;
				}

				if (resultPairs.first().score >= candidatePairs.peek().score) {
					FuzzyKIntMatchingPair cm = resultPairs.pollFirst();
					if (self && cm.stage == -1 && cm.leftInd < cm.rightInd) // from initBucket && can be used again
																			// later
						cachedPairs[cm.rightInd].add(cm.swap());
					if (results != null)
						results.add(new MatchingPair(collection1.keys[cm.leftInd], collection2.keys[cm.rightInd],
								cm.score));
					localTotalMatches++;
					continue;
				}

				FuzzyKIntMatchingPair cm = candidatePairs.poll();
				if (cm.stage == 4) {
					if (self && cm.leftInd < cm.rightInd) { // can be used again later
						cachedPairs[cm.rightInd].add(cm.swap());
					}
					if (cm.score < simThreshold)
						continue;
					resultPairs.add(cm);
					resultPairs.pollLast();
					simThreshold = resultPairs.last().score > limitThreshold ? resultPairs.last().score
							: limitThreshold;
					querySet.computeUnflattenedSignature(simThreshold);
					querySet.computeElementBounds();
				} else {
					if (1 / cm.weightedRatio * simThreshold <= 1.0)
						if (search(cm, simThreshold, idx, querySet.elementBounds, querySet.unflattenedSignature))
							candidatePairs.add(cm);
				}
			}
			searchTime += System.nanoTime() - startTime2;
			totalMatches += localTotalMatches;
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

	protected boolean search(FuzzyKIntMatchingPair cm, double simThreshold, FuzzySetIndex idx, double[] elementBounds,
			TIntList[] unflattenedSignature) {

		int stage = cm.stage;
		if (cm.edges == null)
			cm.lazyInit();

		switch (stage) {
		case 1:
			/* CHECK FILTER */
			startTime = System.nanoTime();
			boolean pass = applyCheckFilter(cm, elementBounds, idx, unflattenedSignature);
			checkFilterTime += System.nanoTime() - startTime;
			cm.stage++; // stage=2
			if (!pass) {
				return false;
			}
			cm.score = cm.weightedRatio * cm.calculateUpperBound(elementBounds);
			totalCheckFilterCandidates += 1;
			break;

		case 2:
			/* NEAREST NEIGHBOR FILTER */
			startTime = System.nanoTime();
			cm.score = applyNNFilter(cm, simThreshold, idx);
			cm.stage++; // stage=3
			nnFilterTime += System.nanoTime() - startTime;
//			if ((cm.score < simThreshold * cm.leftSet.length)
//					|| (cm.score / (cm.leftSet.length + cm.rightSet.length - cm.score) < simThreshold)) {
			if (cm.weightedRatio * cm.score / (cm.leftSet.length + cm.rightSet.length - cm.score) < simThreshold) {
				return false;
			}
			cm.score = cm.weightedRatio * cm.score / (cm.leftSet.length + cm.rightSet.length - cm.score);
			totalNNFilterCandidates += 1;
			break;

		case 3:
			/* VERIFICATION */
			startTime = System.nanoTime();
			cm.score = cm.weightedRatio * cm.evaluate();
			verificationTime += System.nanoTime() - startTime;
			cm.stage++; // stage=4
			if (cm.score < simThreshold)
				return false;
		}

		return true;
	}

	protected boolean applyCheckFilter(FuzzyKIntMatchingPair cm, double[] elementBounds, FuzzySetIndex idx,
			TIntList[] unflattenedSignature) {

		Verification ver = new Verification();
		boolean pass = false;

		double sim;
		// for each element of the query set
		for (int ri = 0; ri < unflattenedSignature.length; ri++) {
			for (int tok : unflattenedSignature[ri].toArray()) {
				if (tok < 0)
					continue;
				if (!idx.idx[tok].containsKey(cm.rightInd))
					continue;
				for (int sj : idx.idx[tok].get(cm.rightInd).toArray()) {
					// compute the similarity score
					sim = ver.verifyWithScore(cm.leftSet[ri], cm.rightSet[sj]);
					cm.addEdge(ri, sj, sim);

					// check the condition
					if (sim >= elementBounds[ri]) {
						totalElements++;
						pass = true;
					}
				}
			}
		}
		return pass;
	}

	protected double applyNNFilter(FuzzyKIntMatchingPair cm, double threshold, FuzzySetIndex idx) {
		threshold = 1 / cm.weightedRatio * threshold;
		TIntSet matchedElements;

		double total = 0;
		matchedElements = new TIntHashSet();
		for (int i = 0; i < cm.leftSet.length; i++) {
			if (cm.nearestNeighbours[i] > 0.0) {
				matchedElements.add(i);
				total += cm.nearestNeighbours[i];
			}
		}

		for (int i = 0; i < cm.leftSet.length; i++) {
			if (matchedElements.contains(i)) {
				continue;
			}

			TIntSet cands = new TIntHashSet();
			for (int t : cm.leftSet[i]) {
				if (t < 0)
					continue;
				TIntSet tempCands = idx.idx[t].get(cm.rightInd);
				if (tempCands != null)
					cands.addAll(tempCands);
			}
			double maxSim = cm.completeNode(i, cands.toArray());

			matchedElements.add(i);
			total += maxSim;

			if (total < threshold * matchedElements.size()) {
				break;
			}
		}
		return total;
	}
}