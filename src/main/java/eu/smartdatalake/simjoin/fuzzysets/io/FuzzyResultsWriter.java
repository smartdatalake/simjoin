package eu.smartdatalake.simjoin.fuzzysets.io;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import eu.smartdatalake.simjoin.fuzzysets.FuzzyJoinResult;

public class FuzzyResultsWriter {

	public void printJoinResults(FuzzyJoinResult result, String outputFile) {
		System.out.println("Total Matches: " + result.totalMatches);

		if (outputFile != null) {
			try {
				PrintWriter writer = new PrintWriter(outputFile);
				if (result.matches != null) {
					for (String key : result.matches.keySet()) {
						writer.println(key + "," + result.matches.get(key));
					}
				} else {
					for (int i = 0; i < result.querySets.length; i++) {
						if (result.matchesPerSet[i] > 0) {
							writer.println(result.querySets[i] + ":" + result.matchesPerSet[i]);
						}
					}
				}
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		try {
			PrintWriter writer = new PrintWriter("stats.txt");
			writer.println("totalMatches=" + result.totalMatches);
			writer.println("joinTime=" + result.joinTime);
			writer.println("leftSize=" + result.leftSize);
			writer.println("rightSize=" + result.rightSize);
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}