package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntMatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.fuzzysets.util.SignatureEvent;
import eu.smartdatalake.simjoin.sets.alg.Verification;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements threshold-based fuzzy set similarity join.
 *
 */
public class ThresholdJoin {
	protected long startTime, transformationTime, indexingTime = 0, joinTime, signatureGenerationTime = 0,
			checkFilterTime = 0, nnFilterTime = 0, verificationTime = 0, searchTime = 0;
	protected int totalCandidates = 0, totalCheckFilterCandidates = 0, totalNNFilterCandidates = 0, totalMatches = 0,
			totalElements = 0;
	private static final Logger logger = LogManager.getLogger(ThresholdJoin.class);

	/**
	 * Implements threshold-based self-join.
	 * 
	 * @param collection
	 *            The input collection.
	 * @param threshold
	 *            The similarity threshold.
	 * @param results
	 *            The queue to which the results are added.
	 */
	public void selfJoin(FuzzyIntSetCollection collection, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, threshold, results);
	}

	/**
	 * Implements threshold-based join.
	 * 
	 * @param collection1
	 *            The left collection.
	 * @param collection2
	 *            The right collection.
	 * @param threshold
	 *            The similarity threshold.
	 * @param results
	 *            The queue to which the results are added.
	 */
	public void join(FuzzyIntSetCollection collection1, FuzzyIntSetCollection collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		boolean self = collection1 == collection2;

		/* CREATE INDEX */
		indexingTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex(collection2);
		indexingTime = System.nanoTime() - indexingTime;
		System.out.println("Indexing time: " + indexingTime / 1000000000.0 + " sec.");

		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection1.sets.length);
		joinTime = System.nanoTime();

		for (int i = 0; i < collection1.sets.length; i++) {
			// progress bar
			pb.progress(joinTime);
			int[][] R = collection1.sets[i];
			int recLength = R.length;

			/* SIGNATURE GENERATION */
			startTime = System.nanoTime();

			SignatureEvent querySet = new SignatureEvent(i, R, idx.costs);
			querySet.computeUnflattenedSignature(threshold);
			querySet.computeElementBounds();

			// compute bounds for length filter
			int recMinLength = (int) Math.ceil(recLength * threshold);
			int recMaxLength = (int) Math.floor(recLength / threshold);

			/* CANDIDATE GENERATION */
			TIntObjectMap<TIntObjectMap<TIntSet>> cands = new TIntObjectHashMap<TIntObjectMap<TIntSet>>();
			TIntSet elem_map;
			TIntObjectMap<TIntSet> set_map;
			for (int ri = 0; ri < querySet.unflattenedSignature.length; ri++) {
				int elemMinLength = (int) Math.ceil(collection1.sets[i][ri].length * threshold);
				int elemMaxLength = (int) Math.floor(collection1.sets[i][ri].length / threshold);
				for (int token : querySet.unflattenedSignature[ri].toArray()) {
					for (int S : idx.idx[token].keys()) {
						if (self && i >= S)
							continue;
						/* SIZE FILTER */
						if (collection2.sets[S].length < recMinLength || collection2.sets[S].length > recMaxLength) {
							continue;
						}

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
							// elem_map.addAll(idx[token].get(S));
							set_map.put(ri, elem_map);
							cands.put(S, set_map);
						}
					}
				}
			}

			signatureGenerationTime += System.nanoTime() - startTime;

			startTime = System.nanoTime();
			for (int c : cands.keys()) {
				FuzzyIntMatchingPair cm = new FuzzyIntMatchingPair(i, c, R, collection2.sets[c], cands.get(c));
				if (search(cm, threshold, idx, querySet.elementBounds)) {
					totalMatches++;
					results.add(
							new MatchingPair(collection1.keys[cm.leftInd], collection2.keys[cm.rightInd], cm.score));
				}
			}
			searchTime += System.nanoTime() - startTime;
		}
		joinTime = System.nanoTime() - joinTime;

		logger.info("Left Size: " + collection1.sets.length);
		logger.info("Right Size: " + collection2.sets.length);
		logger.info("Total Join Time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("\tTransformation Time: " + transformationTime / 1000000000.0 + " sec.");
		logger.info("\tSignature Generation Time: " + signatureGenerationTime / 1000000000.0 + " sec.");
		logger.info("\tSearch Time: " + searchTime / 1000000000.0 + " sec.");
		logger.info("\t\tCheck Filter Time: " + checkFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tNN Filter Time: " + nnFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tVerification Time: " + verificationTime / 1000000000.0 + " sec.");
		logger.info("\tIndexing Time: " + indexingTime / 1000000000.0 + " sec.");

		logger.info("Initial Candidates: " + totalCandidates);
		logger.info("Check Filter Elements: " + totalElements);
		logger.info("Check Filter Candidates: " + totalCheckFilterCandidates);
		logger.info("NN Filter Candidates: " + totalNNFilterCandidates);

		logger.info("Total Matches: " + totalMatches);
	}

	/**
	 * Find matches for a given set
	 */
	protected boolean search(FuzzyIntMatchingPair cm, double threshold, FuzzySetIndex idx, double[] elementBounds) {

		totalCandidates++;
		/* CHECK FILTER */
		long startTime = System.nanoTime();
		boolean pass = applyCheckFilter(cm, threshold, elementBounds);
		checkFilterTime += System.nanoTime() - startTime;
		if (!pass) {
			return false;
		}
		totalCheckFilterCandidates++;

		/* NEAREST NEIGHBOR FILTER */
		startTime = System.nanoTime();
		cm.score = applyNNFilter(cm, threshold, idx);
		nnFilterTime += System.nanoTime() - startTime;
		if ((cm.score < threshold * cm.leftSet.length)
				|| (cm.score / (cm.leftSet.length + cm.rightSet.length - cm.score) < threshold)) {
			return false;
		}
		totalNNFilterCandidates++;

		/* VERIFICATION */
		startTime = System.nanoTime();
		cm.score = cm.evaluate();
		verificationTime += System.nanoTime() - startTime;
		return cm.score >= threshold;
	}

	protected boolean applyCheckFilter(FuzzyIntMatchingPair cm, double threshold, double[] elementBounds) {

		Verification ver = new Verification();
		boolean pass = false;

		double sim;
		// for each element of the query set
		for (int ri : cm.cand.keys()) {
			for (int sj : cm.cand.get(ri).toArray()) {
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
		return pass;
	}

	protected double applyNNFilter(FuzzyIntMatchingPair cm, double threshold, FuzzySetIndex idx) {

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