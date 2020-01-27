package eu.smartdatalake.simjoin.fuzzysets.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.PriorityQueue;

import eu.smartdatalake.simjoin.fuzzysets.util.TokenScore;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.hash.TIntDoubleHashMap;

/**
 * Handles signatures.
 *
 */
public class SignatureEvent {
	private PriorityQueue<TokenScore> tokenScores;
	public TIntList[] unflattenedSignature;
	public double[] elementBounds;
	private double simUpperBound;
	private int[][] querySet;
	public double threshold;

	public SignatureEvent(int index, int[][] querySet, int[] costs) {
		this.querySet = querySet;

		unflattenedSignature = new TIntList[querySet.length];
		for (int r = 0; r < querySet.length; r++) {
			unflattenedSignature[r] = new TIntArrayList();
		}
		elementBounds = new double[querySet.length];

		simUpperBound = querySet.length;

		// Compute token scores
		double score;
		TIntDoubleMap tokenScores2 = new TIntDoubleHashMap();
		// first compute values
		for (int i = 0; i < querySet.length; i++) {
			for (int j = 0; j < querySet[i].length; j++) {
				score = 0;
				if (tokenScores2.containsKey(querySet[i][j])) {
					score = tokenScores2.get(querySet[i][j]);
				}
				score += (1.0 / querySet[i].length);
				tokenScores2.put(querySet[i][j], score);
			}
		}

		tokenScores = new PriorityQueue<TokenScore>();
		// then include costs
		for (int token : tokenScores2.keys()) {
			tokenScores.add(new TokenScore(token, round(costs[token] * round(tokenScores2.get(token), 10), 10)));
		}
	}

	public void computeUnflattenedSignature(double simThreshold) {
		// set threshold and current bound
		double thres = simThreshold * querySet.length;
		int bestToken = -1;

		// construct the signature
		while (simUpperBound >= thres) {
			bestToken = tokenScores.poll().id;
			// update the signature
			for (int i = 0; i < querySet.length; i++) {
				for (int j = 0; j < querySet[i].length; j++) {
					if (querySet[i][j] == bestToken) {
						unflattenedSignature[i].add(bestToken);
						simUpperBound -= (1.0 / (double) querySet[i].length);
					}
				}
			}
		}
		this.threshold = simThreshold;
	}

	public void computeElementBounds() {
		for (int id_r = 0; id_r < querySet.length; id_r++) {
			elementBounds[id_r] = (double) (querySet[id_r].length - unflattenedSignature[id_r].size())
					/ (double) querySet[id_r].length;
		}
	}

	private static double round(double value, int places) {
		BigDecimal bd = new BigDecimal(Double.toString(value));
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
}

class TokenScore implements Comparable<TokenScore> {
	public int id;
	private double score;

	public TokenScore(int id, double score) {
		this.id = id;
		this.score = score;
	}

	@Override
	public int compareTo(TokenScore o) {
		return Double.compare(this.score, o.score);
	}
}