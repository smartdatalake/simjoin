package eu.smartdatalake.simjoin.sets.io;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import gnu.trove.map.TObjectIntMap;
import eu.smartdatalake.simjoin.sets.JoinResult;
import eu.smartdatalake.simjoin.sets.TokenSet;
import eu.smartdatalake.simjoin.sets.TokenSetCollection;
import eu.smartdatalake.simjoin.sets.transform.IntSetCollection;

public class ResultsWriter {

	public static void printRSetCollection(TokenSetCollection collection, int maxLines) {
		int numLines = 0;
		for (TokenSet set : collection.sets) {
			if (maxLines > -1 && numLines > maxLines) {
				break;
			}
			System.out.print(set.id + " | ");
			for (String token : set.tokens) {
				System.out.print(token + " ");
			}
			System.out.println();
			numLines++;
		}
	}

	public static void printTSetCollection(IntSetCollection collection, int maxLines) {
		int numLines = 0;
		for (int[] tokens : collection.sets) {
			if (maxLines > -1 && numLines > maxLines) {
				break;
			}
			System.out.print(collection.idMap.get(numLines) + " | ");
			for (int token : tokens) {
				System.out.print(token + " ");
			}
			System.out.println();
			numLines++;
		}
	}

	public static void printTokenDictionary(TObjectIntMap<String> tokenDictionary, int maxLines) {
		int numLines = 0;
		for (String token : tokenDictionary.keySet()) {
			if (maxLines > -1 && numLines > maxLines) {
				break;
			}
			System.out.println(token + " : " + tokenDictionary.get(token));
			numLines++;
		}
	}

	public void printJoinResults(JoinResult result, String outputFile, String statsFile, boolean self) {
		System.out.println("Total Matches: " + result.totalMatches);

		if (outputFile != null) {
			try {
				PrintWriter writer = new PrintWriter(outputFile);
				if (result.matches != null) {
					for (int i = 0; i < result.querySets.length; i++) {
						if (result.matches[i].length > 0) {
							for (int j = 0; j < result.matches[i].length; j++) {
								if (self && (result.querySets[i].compareTo(result.matches[i][j]) <= 0))
									continue;
								writer.println(result.querySets[i] + "," + result.matches[i][j] + ","
										+ result.matchScores[i][j] + " ");
							}
						}
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
			PrintWriter writer = new PrintWriter(statsFile);
			writer.println("totalMatches=" + result.totalMatches);
			writer.println("joinTime=" + result.joinTime);
			writer.println("leftSize=" + result.leftSize);
			writer.println("rightSize=" + result.rightSize);
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	// /** Sorts a map by its values */
	// public static <K extends Comparable<? super K>, V extends Comparable<?
	// super V>> LinkedHashMap<K, V> sortByValue(
	// Map<K, V> map) {
	// List<Entry<K, V>> list = new ArrayList<Entry<K, V>>(map.entrySet());
	// Comparator<Entry<K, V>> byValue = Entry.comparingByValue();
	// Comparator<Entry<K, V>> byKey = Entry.comparingByKey();
	// Comparator<Entry<K, V>> byValueThenByKey = byValue.thenComparing(byKey);
	// list.sort(byValueThenByKey);
	//
	// LinkedHashMap<K, V> result = new LinkedHashMap<K, V>();
	// for (Entry<K, V> entry : list) {
	// result.put(entry.getKey(), entry.getValue());
	// }
	//
	// return result;
	// }
}