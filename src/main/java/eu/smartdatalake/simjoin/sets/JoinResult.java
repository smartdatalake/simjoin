package eu.smartdatalake.simjoin.sets;

public class JoinResult {
	public long totalMatches;
	public String[] querySets;
	public int[] matchesPerSet;
	public String[][] matches;
	public double[][] matchScores;
	public double joinTime;
	public long leftSize;
	public long rightSize;
}