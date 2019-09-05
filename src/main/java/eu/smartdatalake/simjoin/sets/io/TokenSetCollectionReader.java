package eu.smartdatalake.simjoin.sets.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import eu.smartdatalake.simjoin.sets.TokenSet;
import eu.smartdatalake.simjoin.sets.TokenSetCollection;

public class TokenSetCollectionReader {

	public TokenSetCollection importFromFile(String file, int colSetId, int colSetTokens, String colDelimiter,
			String tokDelimiter, int maxLines, boolean header) {
		TokenSetCollection collection = new TokenSetCollection();
		List<TokenSet> sets = new ArrayList<TokenSet>();

		int lineCount = 0, errorLines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			String[] columns;
			TokenSet set;

			// if the file has header, ignore the first line
			if (header) {
				br.readLine();
			}
			
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && lineCount >= maxLines) {
					break;
				}
				try {
					columns = line.split(colDelimiter);
					set = new TokenSet();
					set.id = String.valueOf(lineCount);
					if (colSetId >= 0) {
						set.id = columns[colSetId];
					}
					set.tokens = new HashSet<String>(Arrays.asList(columns[colSetTokens].split(tokDelimiter)));
					sets.add(set);
					lineCount++;
				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		collection.sets = sets.toArray(new TokenSet[0]);

		double elementsPerSet = 0;
		for (TokenSet set : collection.sets) {
			elementsPerSet += set.tokens.size();
		}
		elementsPerSet /= collection.sets.length;

		System.out.println("Finished reading file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.sets.length + ". Elements per set: " + elementsPerSet);

		return collection;
	}
}