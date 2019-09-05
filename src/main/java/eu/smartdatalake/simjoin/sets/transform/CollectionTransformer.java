package eu.smartdatalake.simjoin.sets.transform;

import java.util.Arrays;

import eu.smartdatalake.simjoin.sets.JoinResult;
import eu.smartdatalake.simjoin.sets.TokenSet;
import eu.smartdatalake.simjoin.sets.TokenSetCollection;
import eu.smartdatalake.simjoin.sets.alg.IntJoinResult;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

public class CollectionTransformer {

	//Provides the document frequency of each token
	public TokenFrequencyPair[] calculateTokenFrequency(TokenSetCollection rawCollection) {

		// Compute token frequencies
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		int frequency = 0;
		for (TokenSet set : rawCollection.sets) {
			for (String token : set.tokens) {
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
	
	
	public TObjectIntMap<String> constructTokenDictionary(TokenSetCollection rawCollection) {

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = calculateTokenFrequency(rawCollection);

		// Assign integer IDs to tokens
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < tfs.length; i++) {
			tokenDict.put(tfs[i].token, i);
		}

		return tokenDict;
	}

	public IntSetCollection transformCollection(TokenSetCollection rawCollection, TObjectIntMap<String> tokenDict) {

		// Transform each raw set
		TokenSet[] rsets = rawCollection.sets;
		IntSet[] tsets = new IntSet[rsets.length];
		IntSet tset;
		TokenSet rset;
		String[] rtokens;
		TObjectIntMap<String> unknownTokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < rsets.length; i++) {
			rset = rsets[i];
			rtokens = rset.tokens.toArray(new String[0]);
			tset = new IntSet();
			tset.id = rset.id;

			// map string tokens to ints
			tset.tokens = new int[rtokens.length];
			for (int j = 0; j < rtokens.length; j++) {
				if (tokenDict.containsKey(rtokens[j])) {
					tset.tokens[j] = tokenDict.get(rtokens[j]);
				} else if (unknownTokenDict.containsKey(rtokens[j])) {
					tset.tokens[j] = unknownTokenDict.get(rtokens[j]);
				} else {
					tset.tokens[j] = -1 * (unknownTokenDict.size() + 1);
					unknownTokenDict.put(rtokens[j], tset.tokens[j]);
				}
			}

			// sort int tokens
			Arrays.sort(tset.tokens);

			tsets[i] = tset;
		}

		// Sort sets by their length and tokens
		Arrays.sort(tsets);

		// Populate the collection
		IntSetCollection collection = new IntSetCollection();
		collection.numTokens = tokenDict.size();
		collection.sets = new int[tsets.length][];
		collection.idMap = new TIntObjectHashMap<>();
		for (int i = 0; i < collection.sets.length; i++) {
			collection.sets[i] = tsets[i].tokens;
			collection.idMap.put(i, tsets[i].id);
		}

		return collection;
	}

	public class TokenFrequencyPair implements Comparable<TokenFrequencyPair> {
		String token;
		int frequency;

		public String getToken() {
			return this.token;
		}
		
		public int getFrequency() {
			return this.frequency;
		}
		
		@Override
		public int compareTo(TokenFrequencyPair tf) {
			int r = this.frequency == tf.frequency ? this.token.compareTo(tf.token) : this.frequency - tf.frequency;
			return r;
		}
	}

	public JoinResult transformResult(IntJoinResult result, TIntObjectMap<String> idMap1,
			TIntObjectMap<String> idMap2) {
		JoinResult joinResult = new JoinResult();
		joinResult.totalMatches = result.totalMatches;
		joinResult.querySets = new String[result.matchesPerSet != null ? result.matchesPerSet.length
				: result.matches.length];
		for (int i = 0; i < joinResult.querySets.length; i++) {
			joinResult.querySets[i] = idMap1.get(i);
		}
		joinResult.matchesPerSet = result.matchesPerSet;
		if (result.matches != null) {
			joinResult.matches = new String[result.matches.length][];
			for (int i = 0; i < result.matches.length; i++) {
				joinResult.matches[i] = new String[result.matches[i].size()];
				for (int j = 0; j < result.matches[i].size(); j++) {
					joinResult.matches[i][j] = idMap2.get(result.matches[i].get(j));
				}
			}
			joinResult.matchScores = new double[result.matchScores.length][];
			for (int i = 0; i < result.matchScores.length; i++) {
				joinResult.matchScores[i] = new double[result.matchScores[i].size()];
				for (int j = 0; j < result.matchScores[i].size(); j++) {
					joinResult.matchScores[i][j] = result.matchScores[i].get(j);
				}
			}
		}
		return joinResult;
	}
}