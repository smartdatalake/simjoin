package eu.smartdatalake.simjoin.fuzzysets;

import eu.smartdatalake.simjoin.fuzzysets.alg.FuzzySetSimJoin;

public class FuzzySetSimJoinImpl implements IFuzzySetSimJoin {

	@Override
	public FuzzyJoinResult rangeSearch(FuzzySet query, FuzzySetCollection collection, double simThreshold,
			boolean returnCounts) {
		FuzzySetCollection collection1 = new FuzzySetCollection(query);
		return rangeJoin(collection1, collection, simThreshold, returnCounts);
	}

	@Override
	public FuzzyJoinResult rangeSelfJoin(FuzzySetCollection collection, double simThreshold, boolean returnCounts) {
		return rangeJoin(null, collection, simThreshold, returnCounts);
	}

	@Override
	public FuzzyJoinResult rangeJoin(FuzzySetCollection collection1, FuzzySetCollection collection2,
			double simThreshold, boolean returnCounts) {

		// Execute the join
		FuzzySetSimJoin fssj = new FuzzySetSimJoin();
		FuzzyJoinResult result = fssj.join(collection1, collection2, simThreshold);

		String stats = "";
		stats += "\nTransformation Time: " + fssj.transformationTime / 1000000000.0 + " sec.\n";
		stats += "Indexing Time: " + fssj.indexingTime / 1000000000.0 + " sec.\n";
		stats += "\nTotal Join Time: " + fssj.joinTime / 1000000000.0 + " sec.\n";
		stats += "Signature Generation Time: " + fssj.signatureGenerationTime / 1000000000.0 + " sec.\n";
		stats += "Check Filter Time: " + fssj.checkFilterTime / 1000000000.0 + " sec.\n";
		stats += "NN Filter Time: " + fssj.nnFilterTime / 1000000000.0 + " sec.\n";
		stats += "Verification Time: " + fssj.verificationTime / 1000000000.0 + " sec.\n\n";
		stats += "Check Filter Candidates: " + fssj.totalCheckFilterCandidates + "\n";
		stats += "NN Filter Candidates: " + fssj.totalNNFilterCandidates + "\n";

		System.out.println(stats);
		result.joinTime = fssj.joinTime / 1000000000.0;
		return result;
	}
}