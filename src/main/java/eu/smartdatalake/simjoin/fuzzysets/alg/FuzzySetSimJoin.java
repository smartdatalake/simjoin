package eu.smartdatalake.simjoin.fuzzysets.alg;

import java.util.HashMap;
import java.util.HashSet;

import gnu.trove.list.TIntList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyJoinResult;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollection;
import eu.smartdatalake.simjoin.fuzzysets.transform.FuzzyCollectionTransformer;
import eu.smartdatalake.simjoin.fuzzysets.transform.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.sets.alg.Verification;

import org.jgrapht.alg.matching.MaximumWeightBipartiteMatching;
import org.jgrapht.graph.*;

public class FuzzySetSimJoin {

	public long startTime, stopTime, transformationTime, indexingTime, joinTime, signatureGenerationTime = 0,
			checkFilterTime = 0, nnFilterTime = 0, verificationTime = 0;
	public int totalCheckFilterCandidates = 0, totalNNFilterCandidates = 0, totalMatches = 0;
	TObjectIntMap<String> tokenDict;
	IndexConstructor ic;

	public FuzzySetSimJoin() {
		tokenDict = new TObjectIntHashMap<String>();
		ic = new IndexConstructor(); 
	}

	/**
	 * Computes the join between two collections
	 */
	public FuzzyJoinResult join(FuzzySetCollection collection1, FuzzySetCollection collection2,
			double simThreshold) {

		/* TRANSFORM THE INPUT COLLECTIONS */
		transformationTime = System.nanoTime();
		FuzzyCollectionTransformer transformer = new FuzzyCollectionTransformer();

//		// Transform input to integer tokens
		FuzzyIntSetCollection transformedCollection2 = transformer.transform(collection2, tokenDict);
		FuzzyIntSetCollection transformedCollection1 = collection1 == null ? transformedCollection2
				: transformer.transform(collection1, tokenDict);

		transformationTime = System.nanoTime() - transformationTime;

		/* JOIN THE TRANSFORMED INPUT COLLECTIONS */
		HashMap<String, Double> matchingPairs = join(transformedCollection1.sets, transformedCollection2.sets,
				simThreshold);

		FuzzyJoinResult result = transformer.transformResult(matchingPairs, transformedCollection1.keys,
				transformedCollection2.keys);

		return result;
	}

	/**
	 * Computes the join between two already transformed and indexed collections
	 */
	public HashMap<String, Double> join(int[][][] collection1, int[][][] collection2, double simThreshold) {

		HashMap<String, Double> matchingPairs = new HashMap<String, Double>();

		/* CREATE INDEX */
		indexingTime = System.nanoTime();
		TIntObjectMap<TIntList>[] idx = ic.buildSetInvertedIndex(collection2, tokenDict.size());
		indexingTime = System.nanoTime() - indexingTime;

		/* EXECUTE THE JOIN ALGORITHM */
		joinTime = System.nanoTime();

		int total_steps = 20;
		int step = collection1.length / total_steps;
		for (int i = 0; i < collection1.length; i++) {
//			 progress bar
			if (collection1.length >= total_steps) {
				if (i % step == 0) {
					System.out.print("|");
					for (int j = 0; j <= (i / step); j++)
						System.out.print("=");
					for (int j = (i / step); j < total_steps; j++)
						System.out.print(" ");
					System.out.print("|" + (i / step * 100) / total_steps + "% \r");
//				System.out.print("|"+"=".repeat(i/step)+" ".repeat(total_steps-i/step)+"|"+(i/step*100)/total_steps+"% \r");
				}
			}
 
			/* SIGNATURE GENERATION */
			startTime = System.nanoTime();
			TIntSet[] unflattenedSignature = computeUnflattenedSignature(collection1[i], simThreshold, idx);
			TIntSet KTR = new TIntHashSet();
			for (TIntSet elem : unflattenedSignature) {
				KTR.addAll(elem);
			}
			TIntSet cands = new TIntHashSet();
			for (int token : KTR.toArray()) {
				if (token >= 0)
					cands.addAll(idx[token].keys());
			}

			// compute the individual element bounds
			double[] elementBounds = new double[collection1[i].length];
			for (int id_r = 0; id_r < collection1[i].length; id_r++) {
				elementBounds[id_r] = (double) (collection1[i][id_r].length - unflattenedSignature[id_r].size())
						/ (double) collection1[i][id_r].length;
			}

			signatureGenerationTime += System.nanoTime() - startTime;

			double sim;
			for (int c : cands.toArray()) {
				SimilarityScores sims = new SimilarityScores(collection2[c].length);
				sim = search(collection1[i], collection2[c], simThreshold, unflattenedSignature, elementBounds, sims);
				if (sim >= simThreshold) {
					totalMatches += 1;
					matchingPairs.put(i + "_" + c, sim);
				}
			}
		}
		joinTime = System.nanoTime() - joinTime;

		return matchingPairs;
	}

	/**
	 * Find matches for a given set
	 */
	protected double search(int[][] querySet, int[][] collection, double simThreshold, TIntSet[] unflattenedSignature, 
			double[] elementBounds, SimilarityScores sims) {

		// compute bounds for length filter
		double minLength = querySet.length * simThreshold;
		double maxLength = querySet.length / simThreshold;

		/* SIZE FILTER */
		if (collection.length < minLength || collection.length > maxLength)
			return 0.0;

		/* CHECK FILTER */
		startTime = System.nanoTime();
		CheckFilterResults checkFilterResults = applyCheckFilter(querySet, collection, unflattenedSignature,
				elementBounds, simThreshold, sims);
		checkFilterTime += System.nanoTime() - startTime;
		if (checkFilterResults == null)
			return 0.0;
		totalCheckFilterCandidates += 1;

		/* NEAREST NEIGHBOR FILTER */
		startTime = System.nanoTime();
		boolean NNPass = applyNNFilter(querySet, collection, checkFilterResults, simThreshold, sims);
		nnFilterTime += System.nanoTime() - startTime;
		if (!NNPass)
			return -1.0;
		totalNNFilterCandidates += 1;

		/* VERIFICATION */
		startTime = System.nanoTime();
		double sim = verifyCandidate(querySet, collection, sims);
		verificationTime += System.nanoTime() - startTime;
		return sim;
	}

	protected TIntSet[] computeUnflattenedSignature(int[][] querySet, double simThreshold,
			TIntObjectMap<TIntList>[] idx) {

		// initialize unflattened signature
		TIntSet[] unflattenedSignature = new TIntHashSet[querySet.length];
		for (int i = 0; i < unflattenedSignature.length; i++) {
			unflattenedSignature[i] = new TIntHashSet();
		}

		// Compute token scores
		double score;
		TIntDoubleMap tokenScores = new TIntDoubleHashMap();
		// first compute values
		for (int i = 0; i < querySet.length; i++) {
			for (int j = 0; j < querySet[i].length; j++) {
				score = 0;
				if (tokenScores.containsKey(querySet[i][j])) {
					score = tokenScores.get(querySet[i][j]);
				}
				score += (1.0 / querySet[i].length);
				tokenScores.put(querySet[i][j], score);
			}
		}
		// then include costs
		int cost;
		for (int token : tokenScores.keys()) {
			if (token < 0) {
				tokenScores.put(token, 0);
			} else {
				cost = 0;
				for (int s : idx[token].keys()) {
					cost += idx[token].get(s).size();
//					cost += idx[token].get(s);
				}
				tokenScores.put(token, cost / tokenScores.get(token));
			}
		}

		// set threshold and current bound
		double thres = simThreshold * querySet.length;
		double simUpperBound = querySet.length;

		// construct the signature
		int bestToken = -1;
		double bestScore = Double.MAX_VALUE;
		while (simUpperBound >= thres) {
			// choose the next best token
			for (int token : tokenScores.keys()) {
				score = tokenScores.get(token);
				if (score < bestScore) {
					bestToken = token;
					bestScore = score;
				}
			}

			// remove it from the pool of tokens and reset best score
			tokenScores.remove(bestToken);
			bestScore = Double.MAX_VALUE;

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

		return unflattenedSignature;
	}

	protected CheckFilterResults applyCheckFilter(int[][] querySet, int[][] collection, TIntSet[] unflattenedSignature,
			double[] elementBounds, double simThreshold, SimilarityScores sims) {

		TIntDoubleMap cachedScores = new TIntDoubleHashMap();
		TIntIntMap cachedElements = new TIntIntHashMap();
		Verification ver = new Verification();

		double sim;
		// for each element of the query set
		for (int i = 0; i < querySet.length; i++) {
			// for each token, retrieve candidates from index
			for (int token : unflattenedSignature[i].toArray()) {
				if (token < 0) {
					continue;
				}
				for (int e = 0; e < collection.length; e++) {
					if (!contains(collection[e], token))
						continue;
					// compute the similarity score
					sim = ver.verifyWithScore(querySet[i], collection[e]);
					sims.add(i, e, sim);
					// check the condition
					if (sim >= elementBounds[i]) {
						if (cachedScores.containsKey(i)) {
							if (sim > cachedScores.get(i)) {
								cachedScores.put(i, sim);
								cachedElements.put(i, e);
							}
						} else {
							cachedScores.put(i, sim);
							cachedElements.put(i, e);
						}
					}
				}
			}
		}

		if (cachedScores.isEmpty())
			return null;
		return new CheckFilterResults(cachedScores, cachedElements);
	}

	public boolean applyNNFilter(int[][] querySet, int[][] collection, CheckFilterResults cfResults,
			double simThreshold, SimilarityScores sims) {

		Verification ver = new Verification();

		double sim, maxSim, total;
		TIntSet matchedElements;

		total = 0;
		matchedElements = new TIntHashSet();
//		System.out.println(cfResults.toString());
		for (int e : cfResults.checkFilterElements.keys()) {
			matchedElements.add(e);
			total += cfResults.checkFilterScores.get(e);
		}

		for (int j = 0; j < querySet.length; j++) {
			if (matchedElements.contains(j)) {
				continue;
			}
			maxSim = 0;
			for (int k = 0; k < collection.length; k++) {
				if (!sims.containsKey(j, k)) {
					sim = ver.verifyWithScore(querySet[j], collection[k]);
					sims.add(j, k, sim);
				} else
					sim = sims.get(j, k);
				if (sim > maxSim) {
					maxSim = sim;
				}
			}
			total += maxSim;
			matchedElements.add(j);
			if (total < simThreshold * matchedElements.size()) {
				return false;
			}
		}
		if (total >= simThreshold * querySet.length) {
			return true;
		}

		return false;

	}

	public double verifyCandidate(int[][] querySet, int[][] probeSet, SimilarityScores sims) {

		SimpleWeightedGraph<String, DefaultWeightedEdge> g = createGraph(querySet, probeSet, sims);

		return evaluateGraph(g);
	}

	private SimpleWeightedGraph<String, DefaultWeightedEdge> createGraph(int[][] querySet, int[][] probeSet,
			SimilarityScores sims) {
		SimpleWeightedGraph<String, DefaultWeightedEdge> g = new SimpleWeightedGraph<String, DefaultWeightedEdge>(
				DefaultWeightedEdge.class);

		Verification ver = new Verification();
		double sim = 0.0;
		for (int id_r = 0; id_r < querySet.length; id_r++) {
			String r_v = "r_" + id_r;
			g.addVertex(r_v);
			for (int id_ss = 0; id_ss < probeSet.length; id_ss++) {
				String s_v = "s_" + id_ss;
				if (!sims.containsKey(id_r, id_ss)) {
					sim = ver.verifyWithScore(querySet[id_r], probeSet[id_ss]);
				} else
					sim = sims.get(id_r, id_ss);

				g.addVertex(s_v);
				DefaultWeightedEdge e = g.addEdge(r_v, s_v);
				g.setEdgeWeight(e, sim);
			}
		}
		return g;
	}

	private double evaluateGraph(SimpleWeightedGraph<String, DefaultWeightedEdge> g) {
		HashSet<String> r_partition = new HashSet<String>();
		HashSet<String> s_partition = new HashSet<String>();

		for (String v : g.vertexSet()) {
			if (v.startsWith("r"))
				r_partition.add(v);
			else if (v.startsWith("s"))
				s_partition.add(v);
		}
		double match = 0.0;
		MaximumWeightBipartiteMatching<String, DefaultWeightedEdge> matching = new MaximumWeightBipartiteMatching<String, DefaultWeightedEdge>(
				g, r_partition, s_partition);
		for (DefaultWeightedEdge ed : matching.getMatching().getEdges())
			match += g.getEdgeWeight(ed);

//		return match / querySet.length;
		return match / (r_partition.size() + s_partition.size() - match);
	}

	private boolean contains(int[] array, int val) {
		for (int i = 0; i < array.length; i++)
			if (array[i] == val)
				return true;
		return false;
	}
}
