package eu.smartdatalake.simjoin.fuzzysets.alg;

import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;

public class CheckFilterResults {
	TIntDoubleMap checkFilterScores;
	TIntIntMap checkFilterElements;

	public CheckFilterResults(TIntDoubleMap checkFilterScores, TIntIntMap checkFilterElements) {
		super();
		this.checkFilterScores = checkFilterScores;
		this.checkFilterElements = checkFilterElements;
	}

	@Override
	public String toString() {
		return "CheckFilterResults [checkFilterScores=" + checkFilterScores + ", checkFilterElements="
				+ checkFilterElements + "]";
	}
}