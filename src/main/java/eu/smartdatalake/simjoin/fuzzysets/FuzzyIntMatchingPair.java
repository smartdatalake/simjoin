package eu.smartdatalake.simjoin.fuzzysets;

import java.util.HashSet;

import org.jgrapht.alg.interfaces.MatchingAlgorithm;
import org.jgrapht.alg.matching.MaximumWeightBipartiteMatching;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import eu.smartdatalake.simjoin.sets.alg.Verification;

/**
 * Contains information for candidate matches.
 *
 */
public class FuzzyIntMatchingPair implements Comparable<FuzzyIntMatchingPair> {
	public int leftInd;
	public int rightInd;
	public double score;
	public int[][] leftSet;
	public int[][] rightSet;
	private SimpleWeightedGraph<ElementVertex, DefaultWeightedEdge> g;
	private double[] edges;
	private int step;
	public double[] nearestNeighborSim;
	public int[] nearestNeighbor;
	public double upperBoundScore;

	public FuzzyIntMatchingPair(int leftInd, int rightInd, int[][] leftSet, int[][] rightSet) {
		this.leftInd = leftInd;
		this.rightInd = rightInd;
		this.score = (double) leftSet.length / rightSet.length;

		this.leftSet = leftSet;
		this.rightSet = rightSet;

		this.step = rightSet.length;
		this.edges = new double[leftSet.length * step];
		nearestNeighborSim = new double[leftSet.length];
		nearestNeighbor = new int[leftSet.length];
		this.upperBoundScore = leftSet.length;
		for (int i = 0; i < nearestNeighbor.length; i++) {
			nearestNeighbor[i] = -1;
		}
	}

	public void addEdge(int i, int j, double sim) {
		if (edges[i * step + j] != 0.0)

			edges[i * step + j] = sim;
		if (sim > nearestNeighborSim[i]) {
			nearestNeighborSim[i] = sim;
			nearestNeighbor[i] = j;
		}
	}

	public double evaluateGraph() {
		HashSet<ElementVertex> r_partition = new HashSet<ElementVertex>();
		HashSet<ElementVertex> s_partition = new HashSet<ElementVertex>();

		for (ElementVertex v : g.vertexSet()) {
			if (v.isLeft(0))
				r_partition.add(v);
			else
				s_partition.add(v);
		}

		double match = 0.0;
		MatchingAlgorithm<ElementVertex, DefaultWeightedEdge> matching = null;
		if (r_partition.size() <= s_partition.size())
			matching = new MaximumWeightBipartiteMatching<ElementVertex, DefaultWeightedEdge>(g, r_partition,
					s_partition);
		else
			matching = new MaximumWeightBipartiteMatching<ElementVertex, DefaultWeightedEdge>(g, s_partition,
					r_partition);

//		matching = new GreedyWeightedMatching<ElementVertex, DefaultWeightedEdge>(cm.g, false);
		for (DefaultWeightedEdge ed : matching.getMatching().getEdges()) {
			match += g.getEdgeWeight(ed);
		}
		score = match / (r_partition.size() + s_partition.size() - match);
		return score;
	}

	public void completeGraph() {
		Verification ver = new Verification();
		double sim = 0.0;
		for (int i = 0; i < leftSet.length; i++) {
			ElementVertex u = new ElementVertex(0, i);
			if (!g.containsVertex(u))
				g.addVertex(u);
			for (int j = 0; j < rightSet.length; j++) {
				ElementVertex v = new ElementVertex(1, j);
				if (!g.containsVertex(v))
					g.addVertex(v);

				sim = edges[i * step + j];
				if (sim == 0.0)
					sim = ver.verifyWithScore(leftSet[i], rightSet[j]);
				if (sim > 0.0)
					g.setEdgeWeight(g.addEdge(u, v), sim);
			}
		}
	}

	public Double getEdge(int i, int j) {
		return edges[i * step + j];
	}

	public boolean existsEdge(int i, int j) {
		return edges[i * step + j] != 0.0;
	}

	private class ElementVertex {
		public int partition;
		public int key;

		public ElementVertex(int partition, int key) {
			this.partition = partition;
			this.key = key;
		}

		public String toString() {
			return partition + "_" + key;
		}

		public int hashCode() {
			return toString().hashCode();
		}

		public boolean equals(Object o) {
			return (o instanceof ElementVertex) && (toString().equals(o.toString()));
		}

		public boolean isLeft(int left) {
			return partition == left;
		}
	}

	@Override
	public int compareTo(FuzzyIntMatchingPair mp) {
		if (score == mp.score) {
			if (leftInd == mp.leftInd)
				return Integer.compare(rightInd, mp.rightInd);
			else
				return Integer.compare(leftInd, mp.leftInd);
		}
		return Double.compare(mp.score, score);
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
		FuzzyIntMatchingPair item = (FuzzyIntMatchingPair) obj;
		return (this.leftInd == item.leftInd) && (this.rightInd == item.rightInd);
	}

	public void evaluate() {
		this.g = new SimpleWeightedGraph<ElementVertex, DefaultWeightedEdge>(DefaultWeightedEdge.class);
		this.completeGraph();
		this.evaluateGraph();
	}
}