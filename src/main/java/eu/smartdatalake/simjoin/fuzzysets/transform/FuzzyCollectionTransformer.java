package eu.smartdatalake.simjoin.fuzzysets.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import eu.smartdatalake.simjoin.fuzzysets.transform.FuzzyIntSetCollection;
import eu.smartdatalake.simjoin.fuzzysets.FuzzyJoinResult;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollection;

public class FuzzyCollectionTransformer {

	/** Computes the frequencies of tokens */
	public LinkedHashMap<String, Integer> computeTokenFrequencies(Collection<List<Set<String>>> collection) {

		LinkedHashMap<String, Integer> tokFreq = new LinkedHashMap<String, Integer>();

		Integer frequency;
		for (List<Set<String>> set : collection) {
			for (Set<String> element : set) {
				for (String token : element) {
					frequency = tokFreq.get(token);
					if (frequency == null) {
						frequency = 0;
					}
					frequency++;
					tokFreq.put(token, frequency);
				}
			}
		}

		tokFreq = sortByValue(tokFreq);

		return tokFreq;
	}

	/** Maps tokens to integer IDs */
	TObjectIntMap<String> mapTokensToInts(LinkedHashMap<String, Integer> tokenFrequencies) {

		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();

		int counter = 0;

		for (String token : tokenFrequencies.keySet()) {
			tokenDict.put(token, counter);
			counter++;
		}

		return tokenDict;
	}

	/** Creates a token dictionary with IDs ordered by frequency */
	public TObjectIntMap<String> mapTokensToIntsByFrequency(Collection<List<Set<String>>> sets) {
		LinkedHashMap<String, Integer> tokenFreq = computeTokenFrequencies(sets);
		TObjectIntMap<String> tokenDict = mapTokensToInts(tokenFreq);
		return tokenDict;
	}

	/** Replaces string tokens with integer IDs */
	public FuzzyIntSetCollection transform(FuzzySetCollection input, TObjectIntMap<String> tokenDictionary) {

		int[][][] collection = new int[input.sets.size()][][];

		boolean existingDictionary = tokenDictionary.size() > 0 ? true : false;
		int unknownTokenCounter = 0;

		int i = 0, j, k;
		List<Set<String>> elements;
		for (String set : input.sets.keySet()) {
			elements = input.sets.get(set);
			collection[i] = new int[elements.size()][];
			j = 0;
			for (Set<String> element : elements) {
				collection[i][j] = new int[element.size()];
				k = 0;
				for (String token : element) {
					if (!tokenDictionary.containsKey(token)) {
						if (existingDictionary) {
							unknownTokenCounter--;
							tokenDictionary.put(token, unknownTokenCounter);
						} else {
							tokenDictionary.put(token, tokenDictionary.size());
						}
					}
					collection[i][j][k] = tokenDictionary.get(token);
					k++;
				}
				Arrays.sort(collection[i][j]);
				j++;
			}
			i++;
		}

		return new FuzzyIntSetCollection(collection, input.sets.keySet());
	}

	/** Sorts a map by its values */
	public static <K extends Comparable<? super K>, V extends Comparable<? super V>> LinkedHashMap<K, V> sortByValue(
			Map<K, V> map) {
		List<Entry<K, V>> list = new ArrayList<Entry<K, V>>(map.entrySet());
		Comparator<Entry<K, V>> byValue = Entry.comparingByValue();
		Comparator<Entry<K, V>> byKey = Entry.comparingByKey();
		Comparator<Entry<K, V>> byValueThenByKey = byValue.thenComparing(byKey);
		list.sort(byValueThenByKey);

		LinkedHashMap<K, V> result = new LinkedHashMap<K, V>();
		for (Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
	}

	public FuzzyJoinResult transformResult(HashMap<String, Double> matchingPairs, Set<String> keys1,
			Set<String> keys2) {
		FuzzyJoinResult result = new FuzzyJoinResult();
		Object[] arkeys1 = keys1.toArray();
		Object[] arkeys2 = keys2.toArray();

		result.totalMatches = matchingPairs.size();
		for (String key : matchingPairs.keySet()) {
			String[] parts = key.split("_");
			String new_key = arkeys1[Integer.parseInt(parts[0])] + "," + arkeys2[Integer.parseInt(parts[1])];
			result.matches.put(new_key, matchingPairs.get(key));
		}
		return result;
	}
}