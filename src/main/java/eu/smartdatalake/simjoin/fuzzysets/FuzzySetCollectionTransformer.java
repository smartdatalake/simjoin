package eu.smartdatalake.simjoin.fuzzysets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Transforms a collection by replacing string tokens with integers assigned
 * based on frequency.
 *
 */
public class FuzzySetCollectionTransformer {

	/**
	 * Creates a dictionary that maps string tokens to integers based on
	 * frequency.
	 * 
	 * @param collection
	 *            The input collection.
	 * @return A dictionary containing the mappings from strings to integers.
	 */
	public TObjectIntMap<String> constructTokenDictionary(GroupCollection<ArrayList<String>> collection) {

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = calculateTokenFrequency(collection);

		// Assign integer IDs to tokens
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < tfs.length; i++) {
			tokenDict.put(tfs[i].token, i);
		}

		return tokenDict;
	}

	private TokenFrequencyPair[] calculateTokenFrequency(GroupCollection<ArrayList<String>> collection) {

		// Compute token frequencies
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		int frequency = 0;
		for (Group<ArrayList<String>> record : collection.groups) {
			for (ArrayList<String> element : record.elements) {
				for (String token : element) {
					frequency = tokenDict.get(token);
					frequency++;
					tokenDict.put(token, frequency);
				}
			}
		}

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = new TokenFrequencyPair[tokenDict.size()];
		TokenFrequencyPair tf;
		int counter = 0;
		for (String token : tokenDict.keySet()) {
			tf = new TokenFrequencyPair();
			tf.token = token;
			tf.frequency = tokenDict.get(token);
			tfs[counter] = tf;
			counter++;
		}

		Arrays.sort(tfs);

		return tfs;
	}

	/**
	 * Transforms a given collection based on the given mappings.
	 * 
	 * @param collection
	 *            The input collection.
	 * @param tokenDictionary
	 *            The dictionary specifying the mappings.
	 * @return The transformed collection.
	 */
	public FuzzyIntSetCollection transformCollection(GroupCollection<ArrayList<String>> collection,
			TObjectIntMap<String> tokenDictionary) {

		boolean existingDictionary = tokenDictionary.size() > 0 ? true : false;
		int unknownTokenCounter = 0;

		int i = 0, j, k;
		ArrayList<ArrayList<String>> elements;
		FuzzyIntSet[] records = new FuzzyIntSet[collection.groups.size()];
		for (Group<ArrayList<String>> group : collection.groups) {
			records[i] = new FuzzyIntSet();
			records[i].id = group.id;
			records[i].weight = group.weight;
			elements = group.elements;
			records[i].tokens = new int[elements.size()][];
			j = 0;
			for (List<String> element : elements) {
				records[i].tokens[j] = new int[element.size()];
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
					records[i].tokens[j][k] = tokenDictionary.get(token);
					k++;
				}
				Arrays.sort(records[i].tokens[j]);
				j++;
			}
			i++;
		}

		Arrays.sort(records);

		// Populate the collection
		FuzzyIntSetCollection transformedCollection = new FuzzyIntSetCollection(tokenDictionary.size(), records.length);

		for (int r = 0; r < transformedCollection.sets.length; r++) {
			transformedCollection.add(records[r], r);
		}

		return transformedCollection;
	}

	private class TokenFrequencyPair implements Comparable<TokenFrequencyPair> {

		private String token;
		private int frequency;

		public int compareTo(TokenFrequencyPair tf) {
			int r = this.frequency == tf.frequency ? this.token.compareTo(tf.token) : this.frequency - tf.frequency;
			return r;
		}

		@Override
		public String toString() {
			return token + "->" + frequency;
		}
	}
}