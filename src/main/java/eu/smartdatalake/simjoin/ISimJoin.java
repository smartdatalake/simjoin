package eu.smartdatalake.simjoin;

import java.util.concurrent.ConcurrentLinkedQueue;

import eu.smartdatalake.simjoin.MatchingPair;

/**
 * Defines the supported similarity join operations.
 *
 * @param <T> The type of the elements in the groups.
 */
public interface ISimJoin<T> {

	/**
	 * Performs a threshold-based self-join operation.
	 * 
	 * @param collection The input collection.
	 * @param threshold  The similarity threshold (between 0 and 1).
	 * @param results    The pairs having similarity above the threshold.
	 */
	public void thresholdJoin(GroupCollection<T> collection, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results);

	/**
	 * Performs a threshold-based join operation.
	 * 
	 * @param collection1 The left collection to join.
	 * @param collection2 The right collection to join.
	 * @param threshold   The similarity threshold (between 0 and 1).
	 * @param results     The pairs having similarity above the threshold.
	 */
	public void thresholdJoin(GroupCollection<T> collection1, GroupCollection<T> collection2, double threshold,
			ConcurrentLinkedQueue<MatchingPair> results);

	/**
	 * Performs a kNN self-join operation.
	 * 
	 * @param collection     The input collection.
	 * @param k              The number of nearest neighbors.
	 * @param limitThreshold A threshold for neighbors to avoid low-scores.
	 * @param results        The k-nearest neighbors for each group in the
	 *                       collection.
	 */
	public void knnJoin(GroupCollection<T> collection, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results);

	/**
	 * Performs a kNN join operation.
	 * 
	 * @param collection1    The left collection to join.
	 * @param collection2    The right collection to join.
	 * @param k              The number of nearest neighbors.
	 * @param limitThreshold A threshold for neighbors to avoid low-scores.
	 * @param results        The k-nearest neighbors for each group in the left
	 *                       collection.
	 */
	public void knnJoin(GroupCollection<T> collection1, GroupCollection<T> collection2, int k, double limitThreshold,
			ConcurrentLinkedQueue<MatchingPair> results);

	/**
	 * Performs a top-k (i.e., k-closest pairs) self-join operation.
	 * 
	 * @param collection The input collection.
	 * @param k          The number of pairs to return.
	 * @param results    The k pairs having the highest similarity score.
	 */
	public void topkJoin(GroupCollection<T> collection, int k, ConcurrentLinkedQueue<MatchingPair> results);

	/**
	 * Performs a top-k (i.e., k-closest pairs) join operation.
	 * 
	 * @param collection1 The left collection to join.
	 * @param collection2 The right collection to join.
	 * @param k           The number of pairs to return.
	 * @param results     The k pairs having the highest similarity score.
	 */
	public void topkJoin(GroupCollection<T> collection1, GroupCollection<T> collection2, int k,
			ConcurrentLinkedQueue<MatchingPair> results);
}