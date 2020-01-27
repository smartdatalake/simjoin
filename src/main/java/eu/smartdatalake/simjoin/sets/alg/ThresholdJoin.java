package eu.smartdatalake.simjoin.sets.alg;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.sets.IntSetCollection;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements threshold-based set similarity join.
 *
 */
public class ThresholdJoin {
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
	public void selfJoin(IntSetCollection collection, double threshold, ConcurrentLinkedQueue<MatchingPair> results) {

		// Initializations
		Verification verification = new Verification();
		long numMatches = 0;

		// Index initialization
		TIntList[] idx = new TIntList[collection.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}

		int minLength, maxLength, candidate, eqoverlap, rPrefixLength, sPrefixLength;
		int[] minOverlap, prefixLength;
		boolean found;

		// Iterate over each probing set
		long joinTime = System.nanoTime();
		int count = 0;
		int rLen = 0, sLen = 0;

		ProgressBar pb = new ProgressBar(collection.sets.length);

		for (int[] r : collection.sets) {
			pb.progress(joinTime);

			// Compute thresholds
			rLen = r.length;
			minLength = (int) Math.ceil(rLen * threshold);
			maxLength = (int) Math.ceil(rLen / threshold);
			minOverlap = new int[maxLength - minLength + 1];
			prefixLength = new int[maxLength - minLength + 1];
			for (int i = 0; i < minOverlap.length; i++) {
				minOverlap[i] = (int) Math
						.ceil(Math.round((threshold / (1 + threshold)) * (rLen + minLength + i) * 100000) / 100000.0);
				prefixLength[i] = rLen - minOverlap[i] + 1;
			}

			// Retrieve and filter candidates
			TIntSet candidates = new TIntHashSet();
			for (int i = 0; i < prefixLength[0]; i++) {
				for (int j = idx[r[i]].size() - 1; j >= 0; j--) {
					candidate = idx[r[i]].get(j);
					if (candidates.contains(candidate))
						continue;

					sLen = collection.sets[candidate].length;

					if (sLen > maxLength - i) {
						candidates.add(candidate);
						continue;
					}

					if (sLen >= minLength) {
						// Apply prefix filter for this specific pair
						eqoverlap = minOverlap[sLen - minLength];
						rPrefixLength = rLen - eqoverlap + 1;
						if (rPrefixLength < i) {
							candidates.add(candidate);
							continue;
						}

						sPrefixLength = sLen - eqoverlap + 1;
						found = false;
						for (int k = 0; k < sPrefixLength; k++) {
							if (collection.sets[candidate][k] == r[i]) {
								found = true;
								break;
							}
						}

						if (found) {
							candidates.add(candidate);

							// Verify candidate
							double score = 0.0;
							eqoverlap = minOverlap[sLen - minLength];
							score = verification.verifyWithScore(r, collection.sets[candidate]);

							if (score >= threshold) {
								// Add the result to the output
								results.add(
										new MatchingPair(collection.keys[count], collection.keys[candidate], score));
								numMatches++;
							}
						}
					} else {
						break;
					}
				}
			}

			// Add probing set to index
			for (int i = 0; i < prefixLength[0]; i++) {
				idx[r[i]].add(count);
			}

			count++;
		}

		joinTime = System.nanoTime() - joinTime;
		logger.info("Size: " + collection.sets.length);
		logger.info("Join algorithm time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("Number of matches: " + numMatches);
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
	public void join(IntSetCollection collection1, IntSetCollection collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Initializations
		Verification verification = new Verification();
		long numMatches = 0;

		// Index construction
		// create an empty inverted list for each token
		TIntList[] tmpIdx = new TIntList[collection2.numTokens];
		for (int i = 0; i < tmpIdx.length; i++) {
			tmpIdx[i] = new TIntArrayList();
		}
		// iterate over the sets and populate the inverted lists
		for (int i = 0; i < collection2.sets.length; i++) {
			int prefixLength = collection2.sets[i].length - (int) Math.ceil(collection2.sets[i].length * threshold) + 1;

			for (int j = 0; j < prefixLength; j++) {
				tmpIdx[collection2.sets[i][j]].add(i);
			}
		}
		// convert index to int[][]
		int[][] idx = new int[collection2.numTokens][];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = tmpIdx[i].toArray();
		}

		long joinTime = System.nanoTime();
		ProgressBar pb = new ProgressBar(collection1.sets.length);

		// Iterate over each probing set
		int count = 0;
		int rLen = 0, sLen = 0;
		for (int[] r : collection1.sets) {
			pb.progress(joinTime);

			int minLength, maxLength, candidate, eqoverlap, rPrefixLength, sPrefixLength;
			int start, end, pos;
			int[] minOverlap, prefixLength;
			boolean found;

			rLen = r.length;

			// Compute thresholds
			minLength = (int) Math.ceil(rLen * threshold);
			maxLength = (int) Math.ceil(rLen / threshold);

			minOverlap = new int[maxLength - minLength + 1];
			prefixLength = new int[maxLength - minLength + 1];
			for (int i = 0; i < minOverlap.length; i++) {
				minOverlap[i] = (int) Math
						.ceil(Math.round((threshold / (1 + threshold)) * (rLen + minLength + i) * 100000) / 100000.0);
				prefixLength[i] = rLen - minOverlap[i] + 1;
			}

			TIntSet candidates = new TIntHashSet();
			for (int i = 0; i < prefixLength[0]; i++) {
				// skip this token if not in the index
				if (r[i] < 0 || r[i] >= idx.length || idx[r[i]].length == 0) {
					continue;
				}

				// Apply the length filter on list boundaries
				sLen = collection2.sets[idx[r[i]][0]].length;
				if (sLen > maxLength)
					continue;

				sLen = collection2.sets[idx[r[i]][idx[r[i]].length - 1]].length;
				if (sLen < minLength)
					continue;

				// Use binary search on minLength to determine where to start
				// searching in the inverted list
				start = 0;
				end = idx[r[i]].length - 1;
				while (start < end) {
					pos = (int) Math.floor((end + start) / 2.0);
					sLen = collection2.sets[idx[r[i]][pos]].length;
					if (sLen < minLength) {
						start = pos + 1;
					} else {
						end = pos - 1;
					}
				}

				sLen = collection2.sets[idx[r[i]][start]].length;
				while (sLen < minLength) {
					start++;
					sLen = collection2.sets[idx[r[i]][start]].length;
				}

				for (int j = start; j < idx[r[i]].length; j++) {
					candidate = idx[r[i]][j];
					if (candidates.contains(candidate))
						continue;
					sLen = collection2.sets[candidate].length;

					// Apply the length filter
					if (sLen > maxLength) {
						candidates.add(candidate);
						break;
					}

					// Apply the prefix filter for this specific pair
					eqoverlap = minOverlap[sLen - minLength];
					rPrefixLength = rLen - eqoverlap + 1;

					if (rPrefixLength >= i) {
						sPrefixLength = sLen - eqoverlap + 1;
						found = false;
						for (int k = 0; k < sPrefixLength; k++) {
							if (collection2.sets[candidate][k] == r[i]) {
								found = true;
								break;
							}
						}
						if (found) {
							candidates.add(candidate);

							// Verify candidate
							double score = 0.0;

							eqoverlap = minOverlap[collection2.sets[candidate].length - minLength];
							score = verification.verifyWithScore(r, collection2.sets[candidate]);

							if (score >= threshold) {
								// Add the result to the output
								results.add(
										new MatchingPair(collection1.keys[count], collection2.keys[candidate], score));
								numMatches++;
							}
						}
					}
				}
			}

			count++;
		}

		joinTime = System.nanoTime() - joinTime;
		logger.info("Left Size: " + collection1.sets.length);
		logger.info("Right Size: " + collection2.sets.length);
		logger.info("Join algorithm time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("Number of matches: " + numMatches);
	}
}