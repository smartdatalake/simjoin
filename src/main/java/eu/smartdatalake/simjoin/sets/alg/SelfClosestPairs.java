package eu.smartdatalake.simjoin.sets.alg;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.math3.util.CombinatoricsUtils;

import eu.smartdatalake.simjoin.sets.transform.IntSetCollection;
import eu.smartdatalake.simjoin.sets.transform.CollectionTransformer.TokenFrequencyPair;
import gnu.trove.list.TIntList;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

public class SelfClosestPairs {

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

		public void add(Pair<Integer, Integer, Double> p) {
			// Find the place in the list where to add
			// this pair and its corresponding similarity score
			int place = this.Pairs.size() - 1;
			while ((place >= 0) && (this.Pairs.get(place).getScore() < p.getScore())) {
				--place;
			}
			place++;

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
			return this.Pairs.get(this.Pairs.size() - 1).getScore();
		}
	}

	class DescendingScoreComparator implements Comparator<Pair<?, ?, ?>> {
		@Override
		public int compare(Pair<?, ?, ?> p1, Pair<?, ?, ?> p2) {
			if (p1.getScore() > p2.getScore())
				return -1;
			if (p1.getScore() < p2.getScore())
				return 1;
			return 0;
		}
	}

	class Item<T> {
		T first, second;

		Item(T p1, T p2) {
			this.first = p1;
			this.second = p2;
		}

		// All classes that have instances used as keys in a hash-like data structure
		// must correctly implement the equals and hashCode methods
		public boolean equals(Object obj) {
			if (!(obj instanceof Item)) {
				return false;
			} else {
				Item<?> that = (Item<?>) obj;
				return this.first.equals(that.first) && this.second.equals(that.second);
			}
		}

		public int hashCode() {
			int hash = this.first.hashCode();
			hash = hash * 31 + this.second.hashCode();
			return hash;
		}

	}

	public IntJoinResult join(IntSetCollection collection, TokenFrequencyPair[] tokenFrequencies, int k) {

		// Priority queue of the prefix events
		PriorityQueue<Pair<Integer, Integer, Double>> prefixEvents = new PriorityQueue<Pair<Integer, Integer, Double>>(
				collection.sets.length, new DescendingScoreComparator());

		// Hash table to remember all candidate pairs that have been verified
		Hashtable<Item<Integer>, Double> verifiedPairs = new Hashtable<Item<Integer>, Double>();

		// Initializations
		Verification verification = new Verification();
		IntJoinResult result = new IntJoinResult();
		result.totalCandidates = 0;
		result.totalMatches = 0;

		result.matches = new TIntArrayList[collection.sets.length];
		result.matchScores = new TDoubleArrayList[collection.sets.length];
		for (int i = 0; i < result.matches.length; i++) {
			result.matches[i] = new TIntArrayList();
			result.matchScores[i] = new TDoubleArrayList();
			// Initialize events -- using identifier of the set in the collection instead of
			// the set itself
			prefixEvents.add(new Pair<Integer, Integer, Double>(i, 1, 1.0));
		}

		double sim, simThreshold;
		CandidatesList resultPairs = new CandidatesList(k);

		// Initialization of temporary results
		// OPTIMIZED: Identify the token with the smallest document frequency that also
		// qualifies with the k constraint
		// This optimization can only be applied if k is less than the number of the
		// input sets
		if (k < collection.sets.length) {
			int mindf = collection.sets.length;
			int seedToken = 0;
			int df;
			for (int j = 0; j < tokenFrequencies.length; j++) {
				df = tokenFrequencies[j].getFrequency();
				if ((df >= 2) && (CombinatoricsUtils.binomialCoefficient(df, 2) >= k) && (df < mindf)) {
					seedToken = j;
					mindf = df;
				}
			}
			int[] seed = new int[mindf]; // Sets that contain the identified token
			int m = 0;
			for (int i = 0; i < collection.sets.length; i++) {
				for (int j = 0; j < collection.sets[i].length; j++) {
					if (collection.sets[i][j] == seedToken) {
						seed[m] = i;
						m++;
					}
				}
			}
			// Generate all possible pairs from the selected sets and use them as temporary
			// results
			for (int i = 0; i < m - 1; i++) {
				for (int j = i + 1; j < m; j++) {
					sim = verification.verifyWithScore(collection.sets[i], collection.sets[j]);
					resultPairs.add(new Pair<Integer, Integer, Double>(i, j, sim));
				}
			}
		} else {
			// BASELINE: Simply pair the first set with the 2nd,..., (k+1)-th subsequent
			// sets in the input collection
			for (int i = 1; i <= Math.min(collection.sets.length - 1, k); i++) {
				sim = verification.verifyWithScore(collection.sets[0], collection.sets[i]);
				resultPairs.add(new Pair<Integer, Integer, Double>(0, i, sim));
			}
		}

		// Index initialization
		TIntList[] idx = new TIntList[collection.numTokens];
		TDoubleList[] sdx = new TDoubleList[collection.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
			sdx[i] = new TDoubleArrayList();
		}

		int minLength = 0, maxLength = 0, count, candidate;
		int px, lx, ly, posx, posy;
		double spx, spy;
		boolean stopIndexInsertions = false;
		int a, b, matches;
		int[] x1, y1;

		// Consume items from the priority queue of events
		while (!prefixEvents.isEmpty()) {
			Pair<Integer, Integer, Double> event = prefixEvents.poll();

			simThreshold = resultPairs.getThreshold();

			if (event.getScore() <= simThreshold)
				break;

			count = event.getLeft(); // Identifier of the probing set
			int[] x = collection.sets[count]; // Probing set
			px = event.getRight();
			spx = 1.0 - ((px - 1) / (1.0 * x.length));
			minLength = (int) Math.ceil(x.length * simThreshold);
			maxLength = (int) Math.ceil(x.length / simThreshold);

			int w = x[px - 1]; // Token to be examined

			// Search the index for this token
			for (int j = 0; j < idx[w].size(); j++) {
				candidate = idx[w].get(j); // Identifier of the candidate set
				int[] y = collection.sets[candidate]; // Candidate set from the index
				spy = sdx[w].get(j);

				// Filtering against access similarity upper bound seems to remove excessive
				// items
				if (((spx * spy) / (spx + spy - (spx * spy))) < simThreshold) {
					idx[w].remove(j, idx[w].size() - j); // Remove entries from the indices
					sdx[w].remove(j, sdx[w].size() - j);
					break;
				}

				// Size filtering
				if ((y.length >= minLength) && (y.length <= maxLength)) {

					// Deal with duplicate pairs with reverse order of their constituents
					a = Math.max(count, candidate); // Max of the identifiers of the probing and the candidate set
					b = Math.min(count, candidate); // Min of the identifiers of the probing and the candidate set
					// Swap order in the pair so that the set with the maximum id becomes the first
					// constituent
					if (a != count) {
						x1 = y;
						y1 = x;
					} else {
						x1 = x;
						y1 = y;
					}

					// Check pair against the hash table
					if (!verifiedPairs.containsKey(new Item<Integer>(a, b))) {
						lx = x1.length - (int) Math.ceil(simThreshold * x1.length) + 1;
						ly = y1.length - (int) Math.ceil(simThreshold * y1.length) + 1;
						sim = verification.verifyWithScore(x1, y1);

						// Check if this pair should be hashed to avoid future re-verification
						posx = 0; // The position of the second common token in a (the first constituent of the
									// pair)
						posy = 0; // The position of the second common token in b (the second constituent of the
									// pair)
						matches = 0;

						for (posx = 0; posx < x1.length; posx++) {
							for (posy = 0; posy < y1.length; posy++) {
								if (x1[posx] == y1[posy]) {
									matches += 1;
									break;
								}
							}
							if (matches == 2)
								break;
						}

						// Pair is hashed to avoid recomputation
						if ((posx <= lx) && (posy <= ly)) {
							verifiedPairs.put(new Item<Integer>(a, b), sim);
						}
						resultPairs.add(new Pair<Integer, Integer, Double>(a, b, sim));
						simThreshold = resultPairs.getThreshold();

					}
				}

				result.totalCandidates += 1;

			}

			// Add probing set to index
			if (!stopIndexInsertions) {

				// Check against the indexing similarity upper bound
				if ((x.length - px + 1) / (1.0 * (x.length + px - 1)) >= simThreshold) {
					idx[w].add(count);
					sdx[w].add(spx);
				} else {
					stopIndexInsertions = true;
				}
			}

			// Update probing similarity upper bound after indexing
			spx = 1.0 - (px / (1.0 * x.length));

			// Push the next token as a new event into the queue
			prefixEvents.add(new Pair<Integer, Integer, Double>(count, px + 1, spx));
		}

		System.out.println("Candidates: " + result.totalCandidates + " Verified pairs: " + verifiedPairs.size()
				+ " Final threshold: " + resultPairs.getThreshold());

		// Collect results to report
		for (int p = 0; p < resultPairs.size(); p++) {
			result.matchScores[resultPairs.get(p).getLeft()].add(resultPairs.get(p).getScore());
			result.matches[resultPairs.get(p).getLeft()].add(resultPairs.get(p).getRight());
		}
		result.totalMatches = resultPairs.size();

		return result;
	}

}
