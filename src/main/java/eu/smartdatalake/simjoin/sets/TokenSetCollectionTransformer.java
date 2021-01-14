package eu.smartdatalake.simjoin.sets;

import java.util.ArrayList;
import java.util.Arrays;

import eu.smartdatalake.simjoin.Group;
import eu.smartdatalake.simjoin.GroupCollection;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

/**
 * Transforms a collection by replacing string tokens with integers assigned
 * based on frequency.
 *
 */
public class TokenSetCollectionTransformer {

	/**
	 * Creates a dictionary that maps string tokens to integers based on
	 * frequency.
	 * 
	 * @param collection
	 *            The input collection.
	 * @return A dictionary containing the mappings from strings to integers.
	 */
	public static TObjectIntMap<String> constructTokenDictionary(GroupCollection<String> collection) {

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = calculateTokenFrequency(collection);

		// Assign integer IDs to tokens
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < tfs.length; i++) {
			tokenDict.put(tfs[i].token, i);
		}

		return tokenDict;
	}

	private static TokenFrequencyPair[] calculateTokenFrequency(GroupCollection<String> collection) {

		// Compute token frequencies
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		int frequency = 0;
		for (Group<String> set : collection.groups) {
			for (String token : set.elements) {
				frequency = tokenDict.get(token);
				frequency++;
				tokenDict.put(token, frequency);
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
	public static IntSetCollection transformCollection(GroupCollection<String> collection,
			TObjectIntMap<String> tokenDictionary) {

		// Transform each original set
		ArrayList<Group<String>> rsets = collection.groups;
		IntSet[] tsets = new IntSet[rsets.size()];
		IntSet tset;
		Group<String> rset;
		String[] rtokens;
		TObjectIntMap<String> unknownTokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < rsets.size(); i++) {
			rset = rsets.get(i);
			rtokens = rset.elements.toArray(new String[0]);
			tset = new IntSet();
			tset.id = rset.id;
			tset.weight = rset.weight;

			// map string tokens to integers
			tset.tokens = new int[rtokens.length];
			for (int j = 0; j < rtokens.length; j++) {
				if (tokenDictionary.containsKey(rtokens[j])) {
					tset.tokens[j] = tokenDictionary.get(rtokens[j]);
				} else if (unknownTokenDict.containsKey(rtokens[j])) {
					tset.tokens[j] = unknownTokenDict.get(rtokens[j]);
				} else {
					tset.tokens[j] = -1 * (unknownTokenDict.size() + 1);
					unknownTokenDict.put(rtokens[j], tset.tokens[j]);
				}
			}

			// sort integer tokens
			Arrays.sort(tset.tokens);

			tsets[i] = tset;
		}

		// Sort sets by their length and tokens
		Arrays.sort(tsets);

		// Populate the collection
		IntSetCollection transformedCollection = new IntSetCollection(tokenDictionary.size(), tsets.length);
		for (int i = 0; i < transformedCollection.sets.length; i++) {
			transformedCollection.add(tsets[i], i);
		}
		return transformedCollection;
	}

	private static class TokenFrequencyPair implements Comparable<TokenFrequencyPair> {

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