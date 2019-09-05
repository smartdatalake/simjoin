package eu.smartdatalake.simjoin.sets.alg;

import java.util.ArrayList;
import java.util.List;

import eu.smartdatalake.simjoin.sets.transform.IntSetCollection;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class ClosestPairs {

	// Auxiliary class that collects qualifying pairs along with their similarity
	// score
	private class Pair<L, R, S> {
		private L l;
		private R r;
		private double score;

		public Pair(L l, R r, double score) {
			this.l = l;
			this.r = r;
			this.score = score;
		}

		public L getLeft() {
			return l;
		}

		public R getRight() {
			return r;
		}

		public double getScore() {
			return score;
		}

		public void setLeft(L l) {
			this.l = l;
		}

		public void setRight(R r) {
			this.r = r;
		}

		public void setScore(double score) {
			this.score = score;
		}

		public String print() {
			return "(" + this.l + ", " + this.r + ")@" + this.score;
		}
	}

	// A list that collects the top-k candidates during search
	// In the end, it contains the final results
	private class CandidatesList {

		private Integer k;
		private List<Pair<Integer, Integer, Double>> Pairs;

		public CandidatesList(int k) {
			this.Pairs = new ArrayList<Pair<Integer, Integer, Double>>();
			this.k = k;
		}

		public Pair<Integer, Integer, Double> get(int i) {
			return this.Pairs.get(i);
		}

		public void add(Pair<Integer, Integer, Double> p, boolean selfJoin) {
			// Find the place in the list where to add
			// this pair and its corresponding similarity score
			int place = this.Pairs.size() - 1;
			while ((place >= 0) && (this.Pairs.get(place).getScore() < p.getScore())) {
				--place;
			}
			place++;

			// Avoid adding duplicate pairs having the inverse order of their constituents
			if ((selfJoin) && (this.Pairs.size() > 0) && (existsInversePair(place, p)))
				return;

			// Add new score and pair to the list
			if (place < this.k) {
				this.Pairs.add(place, p);

				// Expel superfluous item and score from the list
				if (this.Pairs.size() > this.k) {
					this.Pairs.subList(this.k, this.Pairs.size()).clear();
				}
			}
		}

		public int size() {
			return this.Pairs.size();
		}

		public double getThreshold() {
			return this.Pairs.get(this.size() - 1).getScore();
		}

		private boolean existsInversePair(int place, Pair<Integer, Integer, Double> p) {

			int i = place - 1;
			while ((i >= 0) && (this.Pairs.get(i).getScore() <= p.getScore())) {

				if ((this.Pairs.get(i).getLeft().equals(p.getRight()))
						&& (this.Pairs.get(i).getRight().equals(p.getLeft()))) {
					return true;
				}
				--i;
			}
			return false;
		}

	}

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

		CandidatesList resultPairs = new CandidatesList(k);

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

		int minLength = 0, maxLength = 0, candidate, eqoverlap, rPrefixLength, sPrefixLength, prefixBound, i;
		int[] minOverlap = null, prefixLength;
		TIntSet candidates;
		boolean found;
		double sim, ratio;
		double simThreshold = 0.0;

		// Iterate over each probing set
		int count = 0;
		for (int[] r : collection1.sets) {

			// Initialize
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
							start = pos + 1; // search on the right half
						} else {
							end = pos - 1; // search on the left half
						}
					}
					// Specify where to search in the index
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

				while (step * (end - j) > 0) {
					candidate = idx[r[i]].get(j);

					j += step;

					// Used only when binary search is enabled; going to the left (descending order
					// of search)
					if ((j > 0) && (j == end)) {
						j = start - 1;
						end = -1;
						step = -1;
					}

					// Apply length filter and set eqoverlap
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

						resultPairs.add(new Pair<Integer, Integer, Double>(count, candidate, sim), selfJoin);

						// Adjust threshold
						if (resultPairs.size() >= k) {

							simThreshold = resultPairs.getThreshold();
							ratio = simThreshold / (1 + simThreshold);

							// Recompute bounds for prefix filtering
							// based on the updated threshold
							minLength = (int) Math.ceil(r.length * simThreshold);
							maxLength = (int) Math.ceil(r.length / simThreshold);

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
			}

			result.totalCandidates += candidates.size();
			count++;
		}

		System.out.println("Candidates: " + result.totalCandidates);
		System.out.println("Final threshold: " + simThreshold);

		// Collect results to report
		for (int p = 0; p < resultPairs.size(); p++) {
			result.matchScores[resultPairs.get(p).getLeft()].add(resultPairs.get(p).getScore());
			result.matches[resultPairs.get(p).getLeft()].add(resultPairs.get(p).getRight());
		}
		result.totalMatches = resultPairs.size();

		return result;
	}

}
