package eu.smartdatalake.simjoin.fuzzysets.alg;


import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;

public class SimilarityScores {
	TIntDoubleMap scores;
	int lenc; 

	public SimilarityScores(int lenc) {
		this.lenc = lenc;
		scores = new TIntDoubleHashMap();
	}
	
	@Override
	public String toString() {
		return "SimilarityScores [scores=" + scores.toString() + "]";
	}

	public void add(int id_r, int id_ss, double sim) {
		scores.put(id_r*lenc+id_ss, sim);
	}

	public double get(int id_r, int id_ss) {
		return scores.get(id_r*lenc+id_ss);
	}

	public boolean containsKey(int id_r, int id_ss) {
		return scores.containsKey(id_r*lenc+id_ss);
	}
}
