package eu.smartdatalake.simjoin.sets.alg;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.smartdatalake.simjoin.sets.IntSetCollection;
import eu.smartdatalake.simjoin.sets.IntMatchingPair;
import eu.smartdatalake.simjoin.MatchingPair;
import eu.smartdatalake.simjoin.fuzzysets.util.ProgressBar;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

/**
 * Implements top-k set similarity join.
 *
 */
public class TopKJoin {

	boolean optimizedInit = false;
	private static final Logger logger = LogManager.getLogger(TopKJoin.class);

	/**
	 * Implements top-k self-join.
	 * 
	 * @param collection
	 *            The input collection.
	 * @param k
	 *            The number of pairs to return.
	 * @param results
	 *            The queue to which the results are added.
	 */
	public void selfJoin(IntSetCollection collection, int k, ConcurrentLinkedQueue<MatchingPair> results) {
		long joinTime = System.nanoTime();
		// Initializations
		Verification verification = new Verification();
		long numMatches = 0;

		// Priority queue of the prefix events
		PriorityQueue<PrefixEvent> prefixQueue = new PriorityQueue<PrefixEvent>();
		for (int i = 0; i < collection.sets.length; i++) {
			prefixQueue.add(new PrefixEvent(i, collection.sets[i].length));
		}

		// Priority queue of the current matching pairs
		// PriorityQueue<MatchingPair> matchesQueue = new
		// PriorityQueue<MatchingPair>();
		MatchesQueue matchesQueue = new MatchesQueue(k);
		// TODO: Could be replaced with array with lists
		Set<Pair> verified = new HashSet<Pair>();

		// Initialization of temporary results
		// OPTIMIZED: Identify the token with the smallest document frequency
		// that also qualifies with the k constraint
		// This optimization can only be applied if k is less than the number of
		// the input sets
		if (k >= collection.sets.length || !optimizedInit) {
			// BASELINE: Simply pair the first set with the 2nd,..., (k+1)-th
			// subsequent sets in the input collection
			for (int i = 1; i <= Math.min(collection.sets.length - 1, k); i++) {
				double sim = verification.verifyWithScore(collection.sets[0], collection.sets[i]);
				verified.add(new Pair(i, 0));
				matchesQueue.add(new IntMatchingPair(i, 0, sim));
			}
		}
		// TODO: Check if optimizedInit is needed and fetch tokenFrequencies
		// from call
		// else {
		// int mindf = collection.sets.length;
		// int seedToken = 0;
		// int df;
		// for (int j = 0; j < tokenFrequencies.length; j++) {
		// df = tokenFrequencies[j].getFrequency();
		// if ((df >= 2) && (CombinatoricsUtils.binomialCoefficient(df, 2) >= k)
		// && (df < mindf)) {
		// seedToken = j;
		// mindf = df;
		// }
		// }
		// int[] seed = new int[mindf]; // Sets that contain the identified
		// token
		// int m = 0;
		// for (int i = 0; i < collection.sets.length; i++) {
		// for (int j = 0; j < collection.sets[i].length; j++) {
		// if (collection.sets[i][j] == seedToken) {
		// seed[m] = i;
		// m++;
		// }
		// }
		// }
		// // Generate all possible pairs from the selected sets and use them as
		// temporary
		// // results
		// for (int i = 0; i < m - 1; i++) {
		// for (int j = i + 1; j < m; j++) {
		// double sim = verification.verifyWithScore(collection.sets[i],
		// collection.sets[j]);
		// verified.add(new Pair(j, i));
		// matchesQueue.add(new MatchingPair(j, i, sim));
		// }
		// }
		// }

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

		ProgressBar pb = new ProgressBar(k);
		// Consume items from the priority queue of events
		while (!prefixQueue.isEmpty()) {
			PrefixEvent event = prefixQueue.poll();

			double simThreshold = matchesQueue.getThreshold();

			if (event.spx <= simThreshold) {
				break;
			}

			count = event.element; // Identifier of the probing set
			int[] x = collection.sets[count]; // Probing set
			px = event.prefix;
			// spx = 1.0 - ((px - 1) / (1.0 * x.length));
			spx = event.spx;
			minLength = (int) Math.ceil(x.length * simThreshold);
			maxLength = (int) Math.ceil(x.length / simThreshold);

			int w = x[px]; // Token to be examined

			// Search the index for this token
			for (int j = 0; j < idx[w].size(); j++) {
				candidate = idx[w].get(j); // Identifier of the candidate set
				if (candidate >= count)
					continue;
				int[] y = collection.sets[candidate]; // Candidate set from the
														// index
				spy = sdx[w].get(j);

				// Filtering against access similarity upper bound seems to
				// remove excessive items
				if (((spx * spy) / (spx + spy - (spx * spy))) < simThreshold) {
					// Remove entries from the indices
					idx[w].remove(j, idx[w].size() - j);
					sdx[w].remove(j, sdx[w].size() - j);
					break;
				}

				// Size filtering
				if ((y.length >= minLength) && (y.length <= maxLength)) {

					// Deal with duplicate pairs with reverse order of their
					// constituents
					a = Math.max(count, candidate); // Max of the identifiers of
													// the probing and the
													// candidate set
					b = Math.min(count, candidate); // Min of the identifiers of
													// the probing and the
													// candidate set
					// Swap order in the pair so that the set with the maximum
					// id becomes the first constituent
					if (a != count) {
						x1 = y;
						y1 = x;
					} else {
						x1 = x;
						y1 = y;
					}

					// Check pair against the hash table
					if (!verified.contains(new Pair(a, b))) {
						lx = x1.length - (int) Math.ceil(simThreshold * x1.length) + 1;
						ly = y1.length - (int) Math.ceil(simThreshold * y1.length) + 1;
						double sim = verification.verifyWithScore(x1, y1);

						// Check if this pair should be hashed to avoid future
						// re-verification
						posx = 0; // The position of the second common token in
									// a (the first constituent of the
									// pair)
						posy = 0; // The position of the second common token in
									// b (the second constituent of the
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
							verified.add(new Pair(a, b));
						}

						if (sim >= simThreshold) {
							matchesQueue.add(new IntMatchingPair(a, b, sim));
							pb.progress(joinTime);
							simThreshold = matchesQueue.getThreshold();
						}

					}
				}
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

			// Push the next token as a new event into the queue
			if (!event.update() && event.spx >= simThreshold)
				prefixQueue.add(event);
		}

		while (!matchesQueue.isEmpty()) {
			// Add the result to the output
			IntMatchingPair temp = matchesQueue.export();
			results.add(new MatchingPair(collection.keys[temp.leftInd], collection.keys[temp.rightInd], temp.score));
			numMatches++;
		}

		joinTime = System.nanoTime() - joinTime;

		logger.info("Size: " + collection.sets.length);
		logger.info("Join algorithm time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("Number of matches: " + numMatches);
	}

	/**
	 * Implements top-k join.
	 * 
	 * @param collection1
	 *            The left collection.
	 * @param collection2
	 *            The right collection.
	 * @param k
	 *            The number of pairs to return.
	 * @param results
	 *            The queue to which the results are added.
	 */
	public void join(IntSetCollection collection1, IntSetCollection collection2, int k,
			ConcurrentLinkedQueue<MatchingPair> results) {
		long joinTime = System.nanoTime();
		// Initializations
		Verification verification = new Verification();
		long numMatches = 0;

		// Priority queue of the prefix events
		PriorityQueue<PrefixEvent> prefixQueue = new PriorityQueue<PrefixEvent>();
		for (int i = 0; i < collection1.sets.length; i++) {
			prefixQueue.add(new PrefixEvent(i, collection1.sets[i].length));
		}

		// Priority queue of the current matching pairs
		// PriorityQueue<MatchingPair> matchesQueue = new
		// PriorityQueue<MatchingPair>();
		MatchesQueue matchesQueue = new MatchesQueue(k);
		// TODO: Could be replaced with array with lists
		Set<Pair> verified = new HashSet<Pair>();

		// Initialization of temporary results
		// OPTIMIZED: Identify the token with the smallest document frequency
		// that also
		// qualifies with the k constraint
		// This optimization can only be applied if k is less than the number of
		// the
		// input sets
		if (k >= collection1.sets.length || !optimizedInit) {
			// BASELINE: Simply pair the first set with the 2nd,..., (k+1)-th
			// subsequent
			// sets in the input collection
			for (int i = 1; i <= Math.min(collection2.sets.length - 1, k); i++) {
				double sim = verification.verifyWithScore(collection1.sets[0], collection2.sets[i]);
				verified.add(new Pair(0, i));
				matchesQueue.add(new IntMatchingPair(0, i, sim));
			}
		}
		// TODO: Check if optimizedInit is needed and fetch tokenFrequencies
		// from call
		// else {
		// int mindf = collection.sets.length;
		// int seedToken = 0;
		// int df;
		// for (int j = 0; j < tokenFrequencies.length; j++) {
		// df = tokenFrequencies[j].getFrequency();
		// if ((df >= 2) && (CombinatoricsUtils.binomialCoefficient(df, 2) >= k)
		// && (df < mindf)) {
		// seedToken = j;
		// mindf = df;
		// }
		// }
		// int[] seed = new int[mindf]; // Sets that contain the identified
		// token
		// int m = 0;
		// for (int i = 0; i < collection.sets.length; i++) {
		// for (int j = 0; j < collection.sets[i].length; j++) {
		// if (collection.sets[i][j] == seedToken) {
		// seed[m] = i;
		// m++;
		// }
		// }
		// }
		// // Generate all possible pairs from the selected sets and use them as
		// temporary
		// // results
		// for (int i = 0; i < m - 1; i++) {
		// for (int j = i + 1; j < m; j++) {
		// double sim = verification.verifyWithScore(collection.sets[i],
		// collection.sets[j]);
		// verified.add(new Pair(j, i));
		// matchesQueue.add(new MatchingPair(j, i, sim));
		// }
		// }
		// }

		// Index construction
		// create an empty inverted list for each token
		TIntList[] idx = new TIntList[collection2.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}
		// iterate over the sets and populate the inverted lists
		for (int i = 0; i < collection2.sets.length; i++) {
			for (int j = 0; j < collection2.sets[i].length; j++) {
				idx[collection2.sets[i][j]].add(i);
			}
		}

		int minLength = 0, maxLength = 0, count, candidate;
		int px, lx, ly, posx, posy;
		int matches;

		// Consume items from the priority queue of events
		ProgressBar pb = new ProgressBar(k);
		while (!prefixQueue.isEmpty()) {
			PrefixEvent event = prefixQueue.poll();

			double simThreshold = matchesQueue.getThreshold();

			if (event.spx <= simThreshold) {
				break;
			}

			count = event.element; // Identifier of the probing set
			int[] x = collection1.sets[count]; // Probing set
			px = event.prefix;
			// spx = 1.0 - ((px - 1) / (1.0 * x.length));
			minLength = (int) Math.ceil(x.length * simThreshold);
			maxLength = (int) Math.ceil(x.length / simThreshold);

			int w = x[px]; // Token to be examined

			// Search the index for this token
			for (int j = 0; j < idx[w].size(); j++) {
				candidate = idx[w].get(j); // Identifier of the candidate set
				int[] y = collection2.sets[candidate]; // Candidate set from the
														// index

				// Size filtering
				if ((y.length >= minLength) && (y.length <= maxLength)) {

					// Check pair against the hash table
					if (!verified.contains(new Pair(count, candidate))) {
						lx = x.length - (int) Math.ceil(simThreshold * x.length) + 1;
						ly = y.length - (int) Math.ceil(simThreshold * y.length) + 1;
						double sim = verification.verifyWithScore(x, y);

						// Check if this pair should be hashed to avoid future
						// re-verification
						posx = 0; // The position of the second common token in
									// a (the first constituent of the
									// pair)
						posy = 0; // The position of the second common token in
									// b (the second constituent of the
									// pair)
						matches = 0;

						for (posx = 0; posx < x.length; posx++) {
							for (posy = 0; posy < y.length; posy++) {
								if (x[posx] == y[posy]) {
									matches += 1;
									break;
								}
							}
							if (matches == 2)
								break;
						}

						// Pair is hashed to avoid recomputation
						if ((posx <= lx) && (posy <= ly)) {
							verified.add(new Pair(count, candidate));
						}

						if (sim >= simThreshold) {
							matchesQueue.add(new IntMatchingPair(count, candidate, sim));
							pb.progress(joinTime);
							simThreshold = matchesQueue.getThreshold();
						}
					}
				}

			}

			// Push the next token as a new event into the queue
			if (!event.update() && event.spx >= simThreshold)
				prefixQueue.add(event);
		}

		while (!matchesQueue.isEmpty()) {
			// Add the result to the output
			IntMatchingPair temp = matchesQueue.export();
			results.add(new MatchingPair(collection1.keys[temp.leftInd], collection2.keys[temp.rightInd], temp.score));
			numMatches++;
		}

		joinTime = System.nanoTime() - joinTime;

		logger.info("Left Size: " + collection1.sets.length);
		logger.info("Right Size: " + collection2.sets.length);
		logger.info("Join algorithm time: " + joinTime / 1000000000.0 + " sec.");
		logger.info("Number of matches: " + numMatches);
	}

	private class PrefixEvent implements Comparable<PrefixEvent> {
		public int element, length, prefix;
		public double spx;

		public PrefixEvent(int element, int length) {
			this.element = element;
			this.length = length;
			this.prefix = 0;
			this.spx = 1.0;
		}

		public boolean update() {
			prefix++;
			spx -= 1.0 / length;
			return prefix == length;
		}

		public int compareTo(PrefixEvent o) {
			return Double.compare(o.spx, this.spx);
		}

		@Override
		public boolean equals(Object obj) {
			return this.element == ((PrefixEvent) obj).element;
		}

		@Override
		public String toString() {
			return element + ": " + length + " " + prefix + " " + spx;
		}
	}

	private class Pair {
		int leftInd, rightInd;

		public Pair(int leftInd, int rightInd) {
			this.leftInd = leftInd;
			this.rightInd = rightInd;
		}

		public boolean equals(Object obj) {
			if (!(obj instanceof Pair)) {
				return false;
			} else {
				Pair that = (Pair) obj;
				return this.leftInd == that.leftInd && this.rightInd == that.rightInd;
			}
		}

		public int hashCode() {
			return leftInd * 31 + rightInd;
		}
	}

	private class MatchesQueue {

		private Integer k;
		private TreeSet<IntMatchingPair> Q;

		public MatchesQueue(int k) {
			this.Q = new TreeSet<IntMatchingPair>();
			this.k = k;
		}

		public void add(IntMatchingPair mp) {
			Q.add(mp);
			if (Q.size() == (k + 1))
				Q.pollFirst();
		}

		public double getThreshold() {
			if (Q.isEmpty())
				return 0.0;
			return Q.first().score;
		}

		public boolean isEmpty() {
			return Q.isEmpty();
		}

		public IntMatchingPair export() {
			return Q.pollLast();
		}
	}
}