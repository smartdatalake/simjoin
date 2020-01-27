package eu.smartdatalake.simjoin.sets.alg;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Implements the verification stage for set similarity joins.
 *
 */
public class Verification {

	/**
	 * Checks whether the similarity score of a given pair is above the
	 * specified threshold.
	 * 
	 * @param r
	 *            The left set.
	 * @param s
	 *            The right set.
	 * @param minOverlap
	 *            The threshold.
	 * @return Whether the similarity score exceeds the threshold.
	 */
	public boolean verifyJaccardOpt(int[] r, int[] s, int minOverlap) {

		int olap = 0, pr = 0, ps = 0;
		int maxr = r.length - pr + olap;
		int maxs = s.length - ps + olap;

		while (maxr >= minOverlap && maxs >= minOverlap && olap < minOverlap) {
			if (r[pr] == s[ps]) {
				pr++;
				ps++;
				olap++;
			} else if (r[pr] < s[ps]) {
				pr++;
				maxr--;
			} else {
				ps++;
				maxs--;
			}
		}
		return olap >= minOverlap;
	}

	/**
	 * Computes the similarity score of a given pair.
	 * 
	 * @param r
	 *            The left set.
	 * @param s
	 *            The right set.
	 * @return The similarity score.
	 */
	public double verifyWithScore(int[] r, int[] s) {

		int olap = 0, pr = 0, ps = 0;
		int maxr = r.length - pr + olap;
		int maxs = s.length - ps + olap;

		while (maxr > olap && maxs > olap) {

			if (r[pr] == s[ps]) {
				pr++;
				ps++;
				olap++;
			} else if (r[pr] < s[ps]) {
				pr++;
				maxr--;
			} else {
				ps++;
				maxs--;
			}
		}

		return (double) (olap / (1.0 * (r.length + s.length - olap)));
	}

	/**
	 * Computes the similarity score of a given pair.
	 * 
	 * @param r
	 *            The left set.
	 * @param s
	 *            The right set.
	 * @return The similarity score.
	 */
	public double verifyJaccardBasic(int[] r, int[] s) {

		TIntSet intersection = new TIntHashSet(r);
		intersection.retainAll(s);
		TIntSet union = new TIntHashSet(r);
		union.addAll(s);

		return ((double) intersection.size()) / ((double) union.size());
	}
}