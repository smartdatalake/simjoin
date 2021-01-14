package eu.smartdatalake.simjoin.data;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.prepared.PreparedFuzzySet;
import eu.smartdatalake.simjoin.data.prepared.PreparedStandardSet;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.sets.TokenSetCollectionReader;

/**
 * Creates a {@link DataJSONSource} from the given input.
 *
 */
public class DataJSONSource extends DataSource {
	public JSONArray values;
	boolean weight;

	/**
	 * Creates a {@link DataJSONSource} from a CSV file.
	 * 
	 * @param config The configuration of the input
	 * @param mode   The mode of the input, i.e. Standard or Fuzzy
	 * @return A {@link DataJSONSource}.
	 */
	public DataJSONSource(JSONObject config, String mode) {
		super(config, mode, "json");
		values = (JSONArray) config.get("values");
		weight = ((JSONObject) (values.get(0))).containsKey("weight");
	}

	/**
	 * Creates a {@link GroupCollection} from the {@link DataJSONSource}.
	 * 
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection getData(int maxLines) {
		if (mode.equals("fuzzy"))
			return FuzzySetCollectionReader.fromJSON(this, maxLines);
		else
			return TokenSetCollectionReader.fromJSON(this, maxLines);
	}
	
	/**
	 * Prepares the {@link DataSource} by parsing, transforming and creating the index.
	 * 
	 * @param maxLines Number of lines to parse.
	 * @param threshold Threshold for index (only in Standard ThresholdJoin
	 */
	public void prepare(int maxLines, double threshold) {
		if (mode.equals("standard")) {
			prepared = new PreparedStandardSet(TokenSetCollectionReader.fromJSON(this, maxLines), threshold);	
		} else if (mode.equals("fuzzy")) {
			prepared = new PreparedFuzzySet(FuzzySetCollectionReader.fromJSON(this, maxLines));
		}
	}
	
	/**
	 * Whether a weight has been defined or not.
	 * 
	 * @return A {@link boolean}.
	 */
	public boolean existsWeight() {
		return weight;
	}
}
