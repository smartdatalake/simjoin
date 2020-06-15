package eu.smartdatalake.simjoin.fuzzysets;

import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.TIntSet;

import java.util.HashSet;

import org.jgrapht.alg.interfaces.MatchingAlgorithm;
import org.jgrapht.alg.matching.MaximumWeightBipartiteMatching;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import eu.smartdatalake.simjoin.sets.IntMatchingPair;
import eu.smartdatalake.simjoin.sets.alg.Verification;

/**
 * Contains information for candidate matches.
 *
 */
public class FuzzyKIntMatchingPair implements Comparable<FuzzyKIntMatchingPair> {
	public int leftInd;
	public int rightInd;
	public double score;
	public int[][] leftSet;
	public int[][] rightSet;
	public int stage;
	public TIntDoubleMap[] edges;
	public double[] nearestNeighbours;
	public double weightedRatio;

	public FuzzyKIntMatchingPair(int leftInd, int rightInd, int[][] leftSet, int[][] rightSet, double weightedRatio,
			int stage, double score) {
		this.leftInd = leftInd;
		this.rightInd = rightInd;
		this.leftSet = leftSet;
		this.rightSet = rightSet;
		this.weightedRatio = weightedRatio;
		this.stage = stage;
		this.score = score;
	}

	public FuzzyKIntMatchingPair(int leftInd, int rightInd, int[][] leftSet, int[][] rightSet,
			int stage, double leftWeight, double rightWeight) {
		this.leftInd = leftInd;
		this.rightInd = rightInd;
		this.score = (leftSet.length <= rightSet.length) ? ((double) leftSet.length / rightSet.length)
				: ((double) rightSet.length / leftSet.length);

		this.leftSet = leftSet;
		this.rightSet = rightSet;
		this.stage = stage;
		weightedRatio = (leftWeight + rightWeight) / 2.0;
	}

	public FuzzyKIntMatchingPair(int leftInd, int rightInd, int[][] leftSet, int[][] rightSet, double leftWeight,
			double rightWeight) {
		this(leftInd, rightInd, leftSet, rightSet, -1, leftWeight, rightWeight);
	}

	public void lazyInit() {
		this.edges = new TIntDoubleMap[leftSet.length];
		for (int i = 0; i < this.edges.length; i++)
			this.edges[i] = new TIntDoubleHashMap();

		nearestNeighbours = new double[leftSet.length];
		for (int i = 0; i < nearestNeighbours.length; i++) {
			nearestNeighbours[i] = 0.0;
		}
	}

	public void addEdge(int i, int j, double sim) {
		// TODO: directly using put should be sufficient
		if (edges[i].containsKey(j))
			return;

		edges[i].put(j, sim);

		if (sim > nearestNeighbours[i])
			nearestNeighbours[i] = sim;
	}

	public double completeNode(int i, int[] neighbors) {
		Verification ver = new Verification();
		double maxSim = 0.0, sim;
		for (int n : neighbors) {

			if (!edges[i].containsKey(n)) {
				sim = ver.verifyWithScore(leftSet[i], rightSet[n]);
				edges[i].put(n, sim);
			} else {
				sim = edges[i].get(n);
			}
			if (sim >= maxSim) {
				maxSim = sim;
			}
		}
		return maxSim;
	}

	public double calculateUpperBound(double[] elementBounds) {
		double total = 0.0;
		for (int i = 0; i < nearestNeighbours.length; i++) {
			if (nearestNeighbours[i] != 0.0)
				total += nearestNeighbours[i];
			else
				total += elementBounds[i];
		}
		return total / (leftSet.length + rightSet.length - total);
	}

	private class ElementVertex {
		private int partition;
		private int key;

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
	public int compareTo(FuzzyKIntMatchingPair mp) {
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
		FuzzyKIntMatchingPair item = (FuzzyKIntMatchingPair) obj;
		return (this.leftInd == item.leftInd) && (this.rightInd == item.rightInd);
	}

	public IntMatchingPair convert() {
		return new IntMatchingPair(leftInd, rightInd, score);
	}

	public double evaluate() {
		if (edges == null)
			lazyInit();
		
		nearestNeighbours = null;
		SimpleWeightedGraph<ElementVertex, DefaultWeightedEdge> g = new SimpleWeightedGraph<ElementVertex, DefaultWeightedEdge>(DefaultWeightedEdge.class);
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

				if (!edges[i].containsKey(j)) {
					sim = ver.verifyWithScore(leftSet[i], rightSet[j]);
					edges[i].put(j, sim);
				}
				sim = edges[i].get(j);
//				if (sim > nearestNeighbours[i])
//					nearestNeighbours[i] = sim;
				if (sim > 0.0) {
					g.setEdgeWeight(g.addEdge(u, v), sim);
				}
			}
			edges[i] = null;
		}
		edges = null;
		
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

		// matching = new GreedyWeightedMatching<ElementVertex,
		// DefaultWeightedEdge>(cm.g, false);
		for (DefaultWeightedEdge ed : matching.getMatching().getEdges()) {
			match += g.getEdgeWeight(ed);
		}
		score = match / (r_partition.size() + s_partition.size() - match);
		return score;
	}

	public FuzzyKIntMatchingPair swap() {
		return new FuzzyKIntMatchingPair(rightInd, leftInd, rightSet, leftSet, weightedRatio, 4, score);
	}
}