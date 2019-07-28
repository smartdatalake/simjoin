package simjoin.sets.alg;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;

public class IntJoinResult {
	public long totalCandidates, totalMatches;
	public int[] matchesPerSet;
	public TIntList[] matches;
	public TDoubleList[] matchScores;
}