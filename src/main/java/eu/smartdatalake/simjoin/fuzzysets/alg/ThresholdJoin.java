package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntMatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.util.FuzzySetIndex;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.sets.alg.Verification;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
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
			totalElements = 0, weightedSets = 0, weightedCandidates = 0;
	private static final Logger logger = LogManager.getLogger(ThresholdJoin.class);
	private long timeout;
	
	public ThresholdJoin(long timeout) {
		this.timeout = timeout;
	}
	

	/**
	 * Implements threshold-based self-join.
	 * 
	 * @param collection The input collection.
	 * @param threshold  The similarity threshold.
	 * @param results    The queue to which the results are added.
	 */
	public void selfJoin(FuzzyIntSetCollection collection, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, threshold, results);
	}

	/**
	 * Implements threshold-based join.
	 * 
	 * @param collection1 The left collection.
	 * @param collection2 The right collection.
	 * @param threshold   The similarity threshold.
	 * @param results     The queue to which the results are added.
	 */
	public void join(FuzzyIntSetCollection collection1, FuzzyIntSetCollection collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		boolean self = collection1 == collection2;

		/* CREATE INDEX */
		indexingTime = System.nanoTime();
		FuzzySetIndex idx = new FuzzySetIndex(collection2);
		indexingTime = System.nanoTime() - indexingTime;

		/* EXECUTE THE JOIN ALGORITHM */
		ProgressBar pb = new ProgressBar(collection1.sets.length);
		joinTime = System.nanoTime();

		for (int i = 0; i < collection1.sets.length; i++) {
			if (timeout > 0 && joinTime > timeout)
				return;
			// progress bar
			pb.progress(joinTime);

			double weightedThreshold = 2.0 / (collection1.weights[i] + 1) * threshold;
			if (weightedThreshold > 1.0)
				continue;

			int[][] R = collection1.sets[i];
			int recLength = R.length;

			/* SIGNATURE GENERATION */
			startTime = System.nanoTime();

			int[] tokens;
			double[] values;

			// Compute token scores
			double score;
			// first compute values
			TIntDoubleMap valuesMap = new TIntDoubleHashMap();
			for (int ii = 0; ii < R.length; ii++) {
				for (int j = 0; j < R[ii].length; j++) {
					score = 0;
					if (valuesMap.containsKey(R[ii][j])) {
						score = valuesMap.get(R[ii][j]);
					}
					score += (1.0 / R[ii].length);
					valuesMap.put(R[ii][j], score);
				}
			}
			PriorityQueue<TokenScore> tokenScores = new PriorityQueue<TokenScore>();
			// then include costs
			for (int token : valuesMap.keys()) {
				int cost = 0;
				if (token > 0)
					cost = idx.costs[token];
				double val = cost / valuesMap.get(token);
				tokenScores.add(new TokenScore(token, val));
			}

			tokens = new int[tokenScores.size()];
			values = new double[tokenScores.size()];
			int ii = 0;
			while (!tokenScores.isEmpty()) {
				tokens[ii] = tokenScores.poll().id;
				values[ii] = valuesMap.get(tokens[ii]);
				ii++;
			}

			double[][] spxs = new double[tokens.length + 1][];
			spxs[0] = new double[R.length];
			for (int r = 0; r < R.length; r++)
				spxs[0][r] = 1.0;
			double[] upperBounds = new double[tokens.length + 1];
			upperBounds[0] = 1.0 * R.length;

			// compute bounds for length filter
			int recMinLength = (int) Math.ceil(recLength * weightedThreshold);
			int recMaxLength = (int) Math.floor(recLength / weightedThreshold);

			/* CANDIDATE GENERATION */
			TIntSet cands = new TIntHashSet();
			for (int iTok = 1; iTok < tokens.length + 1; iTok++) {
				int token = tokens[iTok - 1];

				TIntSet leftElems = null;
				if (self)
					leftElems = idx.idx[token].get(i);

				if (spxs[iTok] == null) {
					upperBounds[iTok] = 0.0;
					spxs[iTok] = new double[R.length];
					for (int r = 0; r < R.length; r++) {
						spxs[iTok][r] = spxs[iTok - 1][r];
						if ((self && leftElems.contains(r)) || (!self && searchToken(R[r], token)))
							spxs[iTok][r] -= 1.0 / R[r].length;
						upperBounds[iTok] += spxs[iTok][r];

					}
				}

				if (token > 0) {
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

						if (cands.contains(S))
							continue;
						cands.add(S);
						double pairWeightedThreshold = 2.0 / (collection1.weights[i] + collection2.weights[S])
								* threshold;
						if (pairWeightedThreshold > 1.0)
							continue;
						FuzzyIntMatchingPair cm = new FuzzyIntMatchingPair(i, S, R, collection2.sets[S]);

						startTime = System.nanoTime();
						boolean result = search(cm, pairWeightedThreshold, idx, tokens, spxs, iTok, upperBounds, self);
						searchTime += System.nanoTime() - startTime;

						if (result) {
							totalMatches++;
							if (results != null)
								results.add(new MatchingPair(collection1.keys[cm.leftInd],
										collection2.keys[cm.rightInd], cm.score));
						}
					}
				}
				if (upperBounds[iTok] < weightedThreshold * R.length) {
					break;
				}
			}
		}
		joinTime = System.nanoTime() - joinTime;

		logger.info("\tLeft Size: " + collection1.sets.length);
		logger.info("\tRight Size: " + collection2.sets.length);
		logger.info("\tTotal Join Time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("\t\tNN Filter Time: " + nnFilterTime / 1000000000.0 + " sec.");
		logger.info("\t\tVerification Time: " + verificationTime / 1000000000.0 + " sec.");

		System.out.println();
		System.out.println("Initial Candidates: " + totalCandidates);
		logger.info("\tInitial Candidates: " + totalCandidates);
		logger.info("\tNN Filter Candidates: " + totalNNFilterCandidates);

		logger.info("Total Matches: " + totalMatches);
		if (results == null)
			System.out.println("Total Matches: " + totalMatches);
	}

	protected boolean search(FuzzyIntMatchingPair cm, double threshold, FuzzySetIndex idx, int[] tokens,
			double[][] spxs, int pref, double[] upperBounds, boolean self) {

		totalCandidates++;
		long startTime = System.nanoTime();
		Verification ver = new Verification();
		double nn;
		TIntSet elemsToExam = new TIntHashSet();
		for (int r = 0; r < cm.leftSet.length; r++)
			elemsToExam.add(r);

		cm.upperBoundScore = upperBounds[pref - 1];
		for (int iTok = pref; iTok < tokens.length + 1; iTok++) {
			int tok = tokens[iTok - 1];

			TIntSet leftElems = null;
			if (self)
				leftElems = idx.idx[tok].get(cm.leftInd);
			if (spxs[iTok] == null) {
				upperBounds[iTok] = 0.0;
				spxs[iTok] = new double[cm.leftSet.length];
				for (int r = 0; r < cm.leftSet.length; r++) {
					spxs[iTok][r] = spxs[iTok - 1][r];
					if ((self && leftElems.contains(r)) || (!self && searchToken(cm.leftSet[r], tok)))
						spxs[iTok][r] -= 1.0 / cm.leftSet[r].length;
					upperBounds[iTok] += spxs[iTok][r];
				}
			}

			TIntSet rightElems = idx.idx[tok].get(cm.rightInd);
//			for (int r : leftElems.toArray()) {
			for (int r = 0; r < cm.leftSet.length; r++) {
				if (spxs[iTok][r] == spxs[iTok - 1][r]) // token not in r
					continue;
				if (!elemsToExam.contains(r))
					continue;
				nn = cm.nearestNeighborSim[r];
				cm.upperBoundScore -= (nn > spxs[iTok - 1][r]) ? nn : spxs[iTok - 1][r];
				if (rightElems != null) {
					for (int s : rightElems.toArray()) {
						if (cm.existsEdge(r, s))
							continue;
						double sim = ver.verifyWithScore(cm.leftSet[r], cm.rightSet[s]);
						cm.addEdge(r, s, sim);
					}
				}
				nn = cm.nearestNeighborSim[r];
				if (nn >= spxs[iTok][r])
					elemsToExam.remove(r);
				cm.upperBoundScore += (nn > spxs[iTok][r]) ? nn : spxs[iTok][r];
				cm.score = cm.upperBoundScore / (cm.leftSet.length + cm.rightSet.length - cm.upperBoundScore);
				if (threshold - cm.score > .0000001) {
					nnFilterTime += System.nanoTime() - startTime;
					return false;
				}
			}
			if (elemsToExam.isEmpty())
				break;
		}
		nnFilterTime += System.nanoTime() - startTime;
		totalNNFilterCandidates++;

		startTime = System.nanoTime();
		cm.evaluate();
		verificationTime += System.nanoTime() - startTime;
		return cm.score >= threshold;
	}

	public class TokenScore implements Comparable<TokenScore> {
		public int id;
		private double score;

		public TokenScore(int id, double score) {
			this.id = id;
			this.score = score;
		}

		public TokenScore(TokenScore ts) {
			this.id = ts.id;
			this.score = ts.score;
		}

		@Override
		public int compareTo(TokenScore o) {
			return Double.compare(this.score, o.score);
		}

		@Override
		public boolean equals(Object obj) {
			return this.id == ((TokenScore) obj).id;
		}

		@Override
		public int hashCode() {
			return id * 31;
		}
	}

	private final static boolean searchToken(int[] r, int token) {
		for (int tok : r) {
			if (tok == token) {
				return true;
			} else if (tok > token) {
				return false;
			}
		}
		return false;
	}
}