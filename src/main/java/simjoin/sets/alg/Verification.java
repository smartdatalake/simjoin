package simjoin.sets.alg;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class Verification {

	public boolean verifyOpt(int[] r, int[] s, int minOverlap) {

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

	public double verifyWithScore(int[] r, int[] s, int eqoverlap) {

		int olap, pr, ps, maxr, maxs;

		olap = 0;
		pr = 0;
		ps = 0;
		maxr = r.length - pr + olap;
		maxs = s.length - ps + olap;

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

//			System.out.println("eqoverlap: " + eqoverlap + " olap: " + olap + " maxr: " + maxr + " maxs:" + maxs);
		}

		return (double) (olap / (1.0 * (r.length + s.length - olap)));
	}

	public double verifyBasic(int[] r, int[] s) {

		TIntSet intersection = new TIntHashSet(r);
		intersection.retainAll(s);
		TIntSet union = new TIntHashSet(r);
		union.addAll(s);

		return ((double) intersection.size()) / ((double) union.size());
	}
}