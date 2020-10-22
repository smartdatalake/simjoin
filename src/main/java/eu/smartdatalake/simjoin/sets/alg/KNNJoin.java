package eu.smartdatalake.simjoin.sets.alg;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import eu.smartdatalake.simjoin.sets.IntSetCollection;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements kNN set similarity join.
 *
 */
public class KNNJoin {
	private static final Logger logger = LogManager.getLogger(KNNJoin.class);
	private long timeout;

	public KNNJoin(long timeout) {
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
	public void selfJoin(IntSetCollection collection, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results) {
		join(collection, collection, k, limitThreshold, results);
	}

	/**
	 * Implements kNN join.
	 * 
	 * @param collection1 The left collection.
	 * @param collection2 The right collection.
	 * @param k           The number of nearest neighbors to return for each group.
	 * @param limitThreshold A threshold for neighbors to avoid low-scores.
	 * @param results     The queue to which the results are added.
	 */
	public void join(IntSetCollection collection1, IntSetCollection collection2, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results) {

		// Initializations
		Verification verification = new Verification();
		long numMatches = 0;

		// Check whether it is a self-join or not
		boolean selfJoin = (collection1 == collection2);

		// Index initialization
		TIntList[] idx = new TIntList[collection2.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}

		// Index construction
		for (int i = 0; i < collection2.sets.length; i++) {
			// Since no threshold is known beforehand, index is constructed with
			// all tokens (not prefixes)
			for (int j = 0; j < collection2.sets[i].length; j++) {
				idx[collection2.sets[i][j]].add(i);
			}
		}

		int minLength = 0, maxLength = 0, candidate, eqoverlap, rPrefixLength, sPrefixLength, prefixBound, i, place;
		int[] minOverlap = null, prefixLength;
		TIntSet candidates;
		boolean found;
		double sim, simThreshold, ratio;

		// Iterate over each probing set
		ProgressBar pb = new ProgressBar(collection1.sets.length);
		long joinTime = System.nanoTime();
		int count = 0;
		for (int[] r : collection1.sets) {
			if (timeout > 0 && joinTime > timeout)
				return;
			pb.progress(joinTime);

			TIntList matches = new TIntArrayList();
			TDoubleList matchScores = new TDoubleArrayList();

			// Initialize
//			simThreshold = 0.0;
//			double weightedThreshold = 0.0;
			simThreshold = limitThreshold;
			double weightedThreshold = 2.0 / (collection1.weights[count] + 1) * simThreshold;
			eqoverlap = 1;
			prefixBound = r.length;
			
			if (simThreshold > 0.0) {
				if (weightedThreshold > 1.0)
					break;

				ratio = weightedThreshold / (1 + weightedThreshold);

				// Recompute bounds for prefix filtering
				// based on the updated threshold
				minLength = (int) Math.ceil(r.length * weightedThreshold);
				maxLength = (int) Math.floor(r.length / weightedThreshold);

				if (maxLength - minLength + 1 > 0) {
					minOverlap = new int[maxLength - minLength + 1];
					prefixLength = new int[maxLength - minLength + 1];

					for (int p = 0; p < minOverlap.length; p++) {
						minOverlap[p] = (int) Math
								.ceil(Math.round(ratio * (r.length + minLength + p) * 100000) / 100000.0);
						prefixLength[p] = r.length - minOverlap[p] + 1;
					}

					prefixBound = prefixLength[0];
				}
			}
			

			candidates = new TIntHashSet();
			i = 0;

			int j;
			int pos, end, diff_front, diff_rear;
			int start = 0;

			int k_current = k;

			while (i < prefixBound && k_current > 0) {
				// Skip this token if not in the index
				if (r[i] < 0 || r[i] >= idx.length || idx[r[i]].size() == 0) {
					i++;
					continue;
				}

				// Calculate differences in length against the indexed items to
				// determine the search order
				diff_front = r.length - collection2.sets[idx[r[i]].get(0)].length;
				diff_rear = r.length - collection2.sets[idx[r[i]].get(idx[r[i]].size() - 1)].length;

				TIntList consumeCands = new TIntArrayList();

				boolean leftSide = true, rightSide = true;
				if ((diff_front > 0) || (diff_rear < 0)) {
					// Binary search
					start = 0;
					end = idx[r[i]].size() - 1;
					while (start < end) {
						pos = (int) Math.floor((start + end) / 2.0);
						if (collection2.sets[idx[r[i]].get(start)].length < r.length) {
							start = pos + 1; // search on the right part
						} else {
							end = pos - 1; // search on the left part
						}
					}
					// To start searching for candidates in ascending order of
					// length
					j = start;
				} else {
					diff_front = (diff_front > 0) ? 0 : Math.abs(diff_front);
					diff_rear = (diff_rear < 0) ? Math.abs(diff_rear) : 0;

					if (diff_front < diff_rear) {
						// Examine indexed items in ascending order of length
						j = 0;
						leftSide = false;

					} else {
						// Examine indexed items in descending order of length
						j = idx[r[i]].size() - 1;
						rightSide = false;
					}
				}
				consumeCands.add(idx[r[i]].get(j));

				int leftIt = j, rightIt = j;
				while (!consumeCands.isEmpty()) {
					candidate = consumeCands.get(0);
					consumeCands.removeAt(0);

					if (consumeCands.isEmpty()) {
						if (leftSide) {
							leftIt--;
							if (leftIt < 0)
								leftSide = false;
							else {
								consumeCands.add(idx[r[i]].get(leftIt));
							}
						}
						if (rightSide) {
							rightIt++;
							if (rightIt >= idx[r[i]].size()) {
								rightSide = false;
							} else {
								consumeCands.add(idx[r[i]].get(rightIt));
							}
						}
					}

					double doubleWeightedThreshold = 2.0 / (collection1.weights[count] + collection2.weights[candidate])
							* simThreshold;
					if (doubleWeightedThreshold > 1.0) {
						continue;
					}

					int[] S = collection2.sets[candidate];

					// Apply length filter and set eqoverlap, depending on the
					// (ascending/descending) order of search
					if (simThreshold > 0) {
						int sLen = S.length;
						int tempMinLength = (int) Math.ceil(r.length * doubleWeightedThreshold);
						int tempMaxLength = (int) Math.floor(r.length / doubleWeightedThreshold);
						if (sLen < tempMinLength) {
							leftSide = false;
							continue;
						}
						if (sLen > tempMaxLength) {
							rightSide = false;
							continue;
						}
						eqoverlap = minOverlap[sLen - tempMinLength];
					}

					
					// Apply prefix filter
					rPrefixLength = r.length - eqoverlap + 1;
					if (rPrefixLength < i) {
						continue;
					}
					
					sPrefixLength = S.length - eqoverlap + 1;
					found = false;
					for (int m = 0; m < sPrefixLength; m++) {
						if (S[m] == r[i]) {
							found = true;
							break;
						}
					}

					if (!found)
						continue;

					// Skip examination of already seen candidates
					// Exclude identity from kNN results for self-join
					if ((selfJoin && count == candidate) || candidates.contains(candidate)) {
						continue;
					}

					candidates.add(candidate);
					
					// Verify candidate
					sim = verification.verifyWithScore(r, S);
					sim = (collection1.weights[count] + collection2.weights[candidate]) / 2.0 * sim;

					// Update items and scores in the list of results
					if (sim >= simThreshold || matches.size() < k_current) {

						// Find the place in the lists where to add
						// the new score (and the corresponding item)
						place = matchScores.size() - 1;
						while ((place >= 0) && (matchScores.get(place) < sim)) {
							--place;
						}
						place++;

						// Add new score and item to the lists
						if (place < k_current) {
							matchScores.insert(place, sim);
							matches.insert(place, candidate);

							// Expel superfluous item and score from the
							// list
							if (matchScores.size() > k_current) {
								matchScores.removeAt(k_current);
								matches.removeAt(k_current);
							}
						}

						// Adjust threshold
						if (matches.size() >= k_current) {
//							simThreshold = matchScores.get(matchScores.size() - 1);
							simThreshold = matchScores.get(matchScores.size() - 1) > limitThreshold ? matchScores.get(matchScores.size() - 1) : limitThreshold ;
							
							weightedThreshold = 2.0 / (collection1.weights[count] + 1) * simThreshold;
							if (weightedThreshold > 1.0)
								break;

							ratio = weightedThreshold / (1 + weightedThreshold);

							// Recompute bounds for prefix filtering
							// based on the updated threshold
							minLength = (int) Math.ceil(r.length * weightedThreshold);
							maxLength = (int) Math.floor(r.length / weightedThreshold);

							if (maxLength - minLength + 1 > 0) {
								minOverlap = new int[maxLength - minLength + 1];
								prefixLength = new int[maxLength - minLength + 1];

								for (int p = 0; p < minOverlap.length; p++) {
									minOverlap[p] = (int) Math
											.ceil(Math.round(ratio * (r.length + minLength + p) * 100000) / 100000.0);
									prefixLength[p] = r.length - minOverlap[p] + 1;
								}

								prefixBound = prefixLength[0];
							}
						}
					}
				}
				i++;

				// Set the sim upper bound of future matches
				double spx = ((collection1.weights[count] + 1.0) / 2.0) * (1.0 - (i / (1.0 * r.length)));

				// limitThreshold
				if (spx < limitThreshold) {
					while (!matchScores.isEmpty()) {
						if (matchScores.get(0) >= limitThreshold) {
							if (results != null)
								results.add(new MatchingPair(collection1.keys[count], collection2.keys[matches.get(0)],
										matchScores.get(0)));
							k_current--;
							numMatches++;
						}
						matchScores.removeAt(0);
						matches.removeAt(0);
					}
					break;
				}
				
				// Check whether any results can be extracted
				while (matchScores.size() > 0 && matchScores.get(0) >= spx) {
					if (results != null)
						results.add(new MatchingPair(collection1.keys[count], collection2.keys[matches.get(0)],
								matchScores.get(0)));
					k_current--;
					numMatches++;
					matchScores.removeAt(0);
					matches.removeAt(0);
				}
				if (weightedThreshold > 1.0)
					break;
			}
			count++;
		}

		joinTime = System.nanoTime() - joinTime;
		logger.info("Left Size: " + collection1.sets.length);
		logger.info("Right Size: " + collection2.sets.length);
		logger.info("Join algorithm time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("Number of matches: " + numMatches);
		if (results == null)
			System.out.println("Total Matches: " + numMatches);
	}
}