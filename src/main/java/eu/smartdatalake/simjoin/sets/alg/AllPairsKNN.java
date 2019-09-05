package eu.smartdatalake.simjoin.sets.alg;

import eu.smartdatalake.simjoin.sets.transform.IntSetCollection;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class AllPairsKNN {

	public IntJoinResult join(IntSetCollection collection1, IntSetCollection collection2, int k) {

		// Initializations
		Verification verification = new Verification();
		IntJoinResult result = new IntJoinResult();
		result.totalCandidates = 0;
		result.totalMatches = 0;

		result.matches = new TIntArrayList[collection1.sets.length];
		result.matchScores = new TDoubleArrayList[collection1.sets.length];
		for (int i = 0; i < result.matches.length; i++) {
			result.matches[i] = new TIntArrayList();
			result.matchScores[i] = new TDoubleArrayList();
		}

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
		int count = 0;
		for (int[] r : collection1.sets) {

			// Initialize
			simThreshold = 0.0;
			eqoverlap = 1;
			prefixBound = r.length;
			candidates = new TIntHashSet();
			i = 0;

			int j;
			int pos, step, end, diff_front, diff_rear;
			int start = 0;

			while (i < prefixBound) {

				// Calculate differences in length against the indexed items to determine the
				// search order
				diff_front = r.length - collection2.sets[idx[r[i]].get(0)].length;
				diff_rear = r.length - collection2.sets[idx[r[i]].get(idx[r[i]].size() - 1)].length;

				if ((diff_front > 0) || (diff_rear < 0)) {
					// Binary search
					end = idx[r[i]].size() - 1;
					while (start < end) {
						pos = (int) Math.floor((start + end) / 2.0);
						if (collection2.sets[start].length < r.length) {
							start = pos + 1; // search on the right part
						} else {
							end = pos - 1; // search on the left part
						}
					}
					//To start searching for candidates in ascending order of length 
					end = idx[r[i]].size();
					step = 1;
					j = start;

				} else {
					diff_front = (diff_front > 0) ? 0 : Math.abs(diff_front);
					diff_rear = (diff_rear < 0) ? Math.abs(diff_rear) : 0;

					if (diff_front < diff_rear) {
						// Examine indexed items in ascending order of length
						j = 0;
						end = idx[r[i]].size();
						step = 1;
					} else {
						// Examine indexed items in descending order of length
						j = idx[r[i]].size() - 1;
						end = -1;
						step = -1;
					}
				}

				//Search for candidates using the index
				while (step * (end - j) > 0) {
					candidate = idx[r[i]].get(j);

					j += step;
					// Reverse order of search (descending) for candidates in the next iteration
					if ((j > 0) && (j == end)) {
						j = start - 1;
						end = -1;
						step = -1;
					}

					// Apply length filter and set eqoverlap, depending on the
					// (ascending/descending) order of search
					if (simThreshold > 0) {
						if (collection2.sets[candidate].length < minLength) {
							if (step == 1)
								continue;
							else
								break;
						}
						if (collection2.sets[candidate].length > maxLength - i) {
							if (step == 1)
								break;
							else
								continue;
						}
						eqoverlap = minOverlap[collection2.sets[candidate].length - minLength];
					}

					// Apply prefix filter
					rPrefixLength = r.length - eqoverlap + 1;
					if (rPrefixLength < i) {
						continue;
					}

					sPrefixLength = collection2.sets[candidate].length - eqoverlap + 1;
					found = false;
					for (int m = 0; m < sPrefixLength; m++) {
						if (collection2.sets[candidate][m] == r[i]) {
							found = true;
							break;
						}
					}

					if (found) {

						// Skip examination of already seen candidates
						// Exclude identity from kNN results for self-join
						if (candidates.contains(candidate) || (selfJoin && count == candidate)) {
							continue;
						}

						candidates.add(candidate);

						// Verify candidate
						sim = verification.verifyWithScore(r, collection2.sets[candidate]);

						// Update items and scores in the list of results
						if (result.matchScores[count].size() < k || sim > simThreshold) {

							// Find the place in the lists where to add
							// the new score (and the corresponding item)
							place = result.matchScores[count].size() - 1;
							while ((place >= 0) && (result.matchScores[count].get(place) < sim)) {
								--place;
							}
							place++;

							// Add new score and item to the lists
							if (place < k) {
								result.matchScores[count].insert(place, sim);
								result.matches[count].insert(place, candidate);

								// Expel superfluous item and score from the
								// list
								if (result.matchScores[count].size() > k) {
									result.matchScores[count].removeAt(k);
									result.matches[count].removeAt(k);
								}
							}

							// Adjust threshold
							if (result.matchScores[count].size() >= k) {

								simThreshold = result.matchScores[count].get(result.matchScores[count].size() - 1);

								ratio = simThreshold / (1 + simThreshold);

								// Recompute bounds for prefix filtering
								// based on the updated threshold
								minLength = (int) Math.ceil(r.length * simThreshold);
								maxLength = (int) Math.ceil(r.length / simThreshold);

								if (maxLength - minLength + 1 > 0) {
									minOverlap = new int[maxLength - minLength + 1];
									prefixLength = new int[maxLength - minLength + 1];

									for (int p = 0; p < minOverlap.length; p++) {
										minOverlap[p] = (int) Math.ceil(
												Math.round(ratio * (r.length + minLength + p) * 100000) / 100000.0);
										prefixLength[p] = r.length - minOverlap[p] + 1;
									}

									prefixBound = prefixLength[0];
								}
							}
						}
					}
				}
				i++;
			}

			result.totalMatches += result.matches[count].size();
			result.totalCandidates += candidates.size();
			count++;

			// System.out.println(simThreshold);
			// System.out.println(candidates.size());

			// if (count % 250000 == 0) {
			// System.out.println("Count: " + count + " Matches: " +
			// numMatches);
			// }
		}

		System.out.println("Candidates: " + result.totalCandidates);

		return result;
	}

}