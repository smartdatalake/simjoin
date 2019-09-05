package eu.smartdatalake.simjoin.sets.alg;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import eu.smartdatalake.simjoin.sets.transform.IntSetCollection;

public class AllPairs {

	public IntJoinResult join(IntSetCollection collection1, IntSetCollection collection2, double simThreshold,
			boolean returnCounts) {

		// Check whether it is a self-join case
		if (collection1 == collection2) {
			return selfJoin(collection2, simThreshold, returnCounts);
		}

		// Initializations
		Verification verification = new Verification();
		IntJoinResult result = new IntJoinResult();
		result.totalCandidates = 0;
		result.totalMatches = 0;
		if (returnCounts) {
			// will only return number of matches
			result.matchesPerSet = new int[collection1.sets.length];
		} else {
			// will return ids and scores per match
			result.matches = new TIntArrayList[collection1.sets.length];
			result.matchScores = new TDoubleArrayList[collection1.sets.length];
			for (int i = 0; i < result.matches.length; i++) {
				result.matches[i] = new TIntArrayList();
				result.matchScores[i] = new TDoubleArrayList();
			}
		}

		// Index construction
		// create an empty inverted list for each token
		TIntList[] tmpIdx = new TIntList[collection2.numTokens];
		for (int i = 0; i < tmpIdx.length; i++) {
			tmpIdx[i] = new TIntArrayList();
		}
		// iterate over the sets and populate the inverted lists
		for (int i = 0; i < collection2.sets.length; i++) {
			int prefixLength = collection2.sets[i].length - (int) Math.ceil(collection2.sets[i].length * simThreshold)
					+ 1;
			for (int j = 0; j < prefixLength; j++) {
				tmpIdx[collection2.sets[i][j]].add(i);
			}
		}
		// convert index to int[][]
		int[][] idx = new int[collection2.numTokens][];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = tmpIdx[i].toArray();
		}

		double ratio = simThreshold / (1 + simThreshold);

		// Iterate over each probing set
		int count = 0;
		for (int[] r : collection1.sets) {

			int minLength, maxLength, candidate, eqoverlap, rPrefixLength, sPrefixLength;
			int start, end, pos;
			int[] minOverlap, prefixLength;
			TIntSet candidates = new TIntHashSet();
			boolean found;

			// Compute thresholds
			minLength = (int) Math.ceil(r.length * simThreshold);
			maxLength = (int) Math.ceil(r.length / simThreshold);
			minOverlap = new int[maxLength - minLength + 1];
			prefixLength = new int[maxLength - minLength + 1];
			for (int i = 0; i < minOverlap.length; i++) {
				// minOverlap[i] = (int) Math.ceil(ratio * (r.length + minLength
				// + i));
				minOverlap[i] = (int) Math.ceil(Math.round(ratio * (r.length + minLength + i) * 100000) / 100000.0);
				prefixLength[i] = r.length - minOverlap[i] + 1;
			}

			for (int i = 0; i < prefixLength[0]; i++) {

				// skip this token if not in the index
				if (r[i] < 0 || r[i] >= idx.length || idx[r[i]].length == 0) {
					continue;
				}

				// Apply the length filter on list boundaries
				if (collection2.sets[idx[r[i]][0]].length > maxLength
						|| collection2.sets[idx[r[i]][idx[r[i]].length - 1]].length < minLength) {
					continue;
				}

				// Use binary search on minLength to determine where to start
				// searching in the inverted list
				start = 0;
				end = idx[r[i]].length - 1;
				while (start < end) {
					pos = (int) Math.floor((end + start) / 2.0);
					if (collection2.sets[idx[r[i]][pos]].length < minLength) {
						start = pos + 1;
					} else {
						end = pos - 1;
					}
				}
				while (collection2.sets[idx[r[i]][start]].length < minLength) {
					start++;
				}

				for (int j = start; j < idx[r[i]].length; j++) {
					candidate = idx[r[i]][j];

					// Apply the length filter
					if (collection2.sets[candidate].length > maxLength) {
						break;
					}

					// Apply the prefix filter for this specific pair
					eqoverlap = minOverlap[collection2.sets[candidate].length - minLength];
					rPrefixLength = r.length - eqoverlap + 1;

					if (rPrefixLength >= i) {
						sPrefixLength = collection2.sets[candidate].length - eqoverlap + 1;
						found = false;
						for (int k = 0; k < sPrefixLength; k++) {
							if (collection2.sets[candidate][k] == r[i]) {
								found = true;
								break;
							}
						}
						if (found) {
							candidates.add(candidate);
						}
					}
				}
			}

			// Verify candidates
			double score;
			for (int c : candidates.toArray()) {

				eqoverlap = minOverlap[collection2.sets[c].length - minLength];
				// score = verification.verifyBasic(r, collection2.sets[c]);
				score = verification.verifyWithScore(r, collection2.sets[c]);
				if (score >= simThreshold) {
					if (returnCounts) {
						result.matchesPerSet[count]++;
					} else {
						result.matches[count].add(c);
						result.matchScores[count].add(score);
					}
					result.totalMatches++;
				}
			}

			count++;

			// if (count % 250000 == 0) {
			// System.out.println("Count: " + count + " Matches: " +
			// numMatches);
			// }

			result.totalCandidates += candidates.size();
		}

		return result;
	}

	private IntJoinResult selfJoin(IntSetCollection collection, double simThreshold, boolean returnCounts) {

		// Initializations
		Verification verification = new Verification();
		IntJoinResult result = new IntJoinResult();
		result.totalCandidates = 0;
		result.totalMatches = 0;
		if (returnCounts) {
			result.matchesPerSet = new int[collection.sets.length];
		} else {
			result.matches = new TIntArrayList[collection.sets.length];
			result.matchScores = new TDoubleArrayList[collection.sets.length];
			for (int i = 0; i < result.matches.length; i++) {
				result.matches[i] = new TIntArrayList();
				result.matchScores[i] = new TDoubleArrayList();
			}
		}

		// Index initialization
		TIntList[] idx = new TIntList[collection.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}

		int minLength, maxLength, candidate, eqoverlap, rPrefixLength, sPrefixLength;
		int[] minOverlap, prefixLength;
		TIntSet candidates;
		boolean found;

		double ratio = simThreshold / (1 + simThreshold);

		// Iterate over each probing set
		int count = 0;
		for (int[] r : collection.sets) {

			// Compute thresholds
			minLength = (int) Math.ceil(r.length * simThreshold);
			maxLength = (int) Math.ceil(r.length / simThreshold);

			minOverlap = new int[maxLength - minLength + 1];
			prefixLength = new int[maxLength - minLength + 1];

			for (int i = 0; i < minOverlap.length; i++) {
				// minOverlap[i] = (int) Math.ceil(ratio * (r.length + minLength
				// + i));
				minOverlap[i] = (int) Math.ceil(Math.round(ratio * (r.length + minLength + i) * 100000) / 100000.0);
				prefixLength[i] = r.length - minOverlap[i] + 1;
			}

			// Retrieve and filter candidates
			candidates = new TIntHashSet();
			for (int i = 0; i < prefixLength[0]; i++) {
				for (int j = idx[r[i]].size() - 1; j >= 0; j--) {
					candidate = idx[r[i]].get(j);

					if (collection.sets[candidate].length > maxLength - i) {
						continue;
					}

					if (collection.sets[candidate].length >= minLength) {
						// apply prefix filter for this specific pair
						eqoverlap = minOverlap[collection.sets[candidate].length - minLength];
						rPrefixLength = r.length - eqoverlap + 1;
						if (rPrefixLength < i) {
							continue;
						}

						sPrefixLength = collection.sets[candidate].length - eqoverlap + 1;
						found = false;
						for (int k = 0; k < sPrefixLength; k++) {
							if (collection.sets[candidate][k] == r[i]) {
								found = true;
								break;
							}
						}
						if (found) {
							candidates.add(candidate);
						}
					} else {
						break;
					}
				}
			}

			// Verify candidates
			double score;
			for (int c : candidates.toArray()) {

				eqoverlap = minOverlap[collection.sets[c].length - minLength];
				// score = verification.verifyBasic(r, collection2.sets[c]);
				score = verification.verifyWithScore(r, collection.sets[c]);
				if (score >= simThreshold) {
					if (returnCounts) {
						result.matchesPerSet[count]++;
						result.matchesPerSet[c]++;
					} else {
						result.matches[count].add(c);
						result.matchScores[count].add(score);
						result.matches[c].add(count);
						result.matchScores[c].add(score);
					}
					result.totalMatches++;
				}
			}

			// Add probing set to index
			for (int i = 0; i < prefixLength[0]; i++) {
				idx[r[i]].add(count);
			}

			count++;

			// if (count % 250000 == 0) {
			// System.out.println("Count: " + count + " Matches: " +
			// numMatches);
			// }

			result.totalCandidates += candidates.size();
		}

		return result;
	}
}