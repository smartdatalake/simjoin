package simjoin.sets.alg;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import simjoin.sets.transform.IntSetCollection;

public class AllPairs {

	public IntJoinResult join(IntSetCollection collection1, IntSetCollection collection2, double simThreshold,
			boolean returnCounts) {

		Verification verification = new Verification();
		IntJoinResult result = new IntJoinResult();
		result.totalCandidates = 0;
		result.totalMatches = 0;
		if (returnCounts) {
			result.matchesPerSet = new int[collection1.sets.length];
		} else {
			result.matches = new TIntArrayList[collection1.sets.length];
			result.matchScores = new TDoubleArrayList[collection1.sets.length];
			for (int i = 0; i < result.matches.length; i++) {
				result.matches[i] = new TIntArrayList();
				result.matchScores[i] = new TDoubleArrayList();
			}
		}

		boolean selfJoin = (collection1 == collection2);

		// Index initialization
		TIntList[] idx = new TIntList[collection2.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}
		if (!selfJoin) {
			// Index construction
			for (int i = 0; i < collection2.sets.length; i++) {
				int prefixLength = collection2.sets[i].length
						- (int) Math.ceil(collection2.sets[i].length * simThreshold) + 1;
				for (int j = 0; j < prefixLength; j++) {
					idx[collection2.sets[i][j]].add(i);
				}
			}
		}

		int minLength, maxLength, candidate, eqoverlap, rPrefixLength, sPrefixLength;
		int[] minOverlap, prefixLength;
		TIntSet candidates;
		boolean found;

		double ratio = simThreshold / (1 + simThreshold);

		// iterate over each probing set
		int count = 0;
		for (int[] r : collection1.sets) {

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

					if (collection2.sets[candidate].length > maxLength - i) {
						continue;
					}

					if (collection2.sets[candidate].length >= minLength) {
						// apply prefix filter for this specific pair
						eqoverlap = minOverlap[collection2.sets[candidate].length - minLength];
						rPrefixLength = r.length - eqoverlap + 1;
						if (rPrefixLength < i) {
							continue;
						}

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
					} else {
						break;
					}
				}
			}

			// Verify candidates
			double score;
			for (int c : candidates.toArray()) {

				eqoverlap = minOverlap[collection2.sets[c].length - minLength];
//				score = verification.verifyBasic(r, collection2.sets[c]);
				score = verification.verifyWithScore(r, collection2.sets[c], eqoverlap);
				if (score >= simThreshold) {
					if (returnCounts) {
						result.matchesPerSet[count]++;
						if (selfJoin) {
							result.matchesPerSet[c]++;
						}
					} else {						
						result.matches[count].add(c);
						result.matchScores[count].add(score);
						if (selfJoin) {
							result.matches[c].add(count);
							result.matchScores[c].add(score);
						}
					}
					result.totalMatches++;
				}
			}

			// Self-join: add probing set to index
			if (selfJoin) {
				for (int i = 0; i < prefixLength[0]; i++) {
					idx[r[i]].add(count);
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
}