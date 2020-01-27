package eu.smartdatalake.simjoin;

/**
 * Represents a pair of groups in the join result.
 *
 */
public class MatchingPair {

	/**
	 * The identifier of the left group.
	 */
	public String leftID;
	/**
	 * The identifier of the right group.
	 */
	public String rightID;
	/**
	 * The similarity score of the pair.
	 */
	public double score;

	public MatchingPair(String leftID, String rightID, double score) {
		this.leftID = leftID;
		this.rightID = rightID;
		this.score = score;
	}

	@Override
	public String toString() {
		return leftID + "," + rightID + "," + score;
	}
}