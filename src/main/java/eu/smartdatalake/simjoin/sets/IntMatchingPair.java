package eu.smartdatalake.simjoin.sets;

/**
 * Represents a matching pair with integer identifiers assigned internally.
 *
 */
public class IntMatchingPair implements Comparable<IntMatchingPair> {

	public int leftInd;
	public int rightInd;
	public double score;

	public IntMatchingPair(int leftInd, int rightInd, double score) {
		this.leftInd = leftInd;
		this.rightInd = rightInd;
		this.score = score;
	}

	public IntMatchingPair(int leftInd, int rightInd) {
		this.leftInd = leftInd;
		this.rightInd = rightInd;
	}

	public int compareTo(IntMatchingPair mp) {
		if (this.leftInd == mp.leftInd && this.rightInd == mp.rightInd)
			return 0;
		if (score == mp.score) {
			if (leftInd == mp.leftInd)
				return Integer.compare(rightInd, mp.rightInd);
			else
				return Integer.compare(leftInd, mp.leftInd);
		}
		return Double.compare(score, mp.score);
	}

	@Override
	public String toString() {
		return leftInd + "," + rightInd + "," + score;
	}

	@Override
	public int hashCode() {
		return leftInd * 31 + rightInd;
	}

	@Override
	public boolean equals(Object obj) {
		IntMatchingPair item = (IntMatchingPair) obj;
		return (this.leftInd == item.leftInd) && (this.rightInd == item.rightInd);
	}
}