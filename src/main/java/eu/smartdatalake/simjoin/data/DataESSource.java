package eu.smartdatalake.simjoin.data;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.GroupCollection;
import eu.smartdatalake.simjoin.data.prepared.PreparedFuzzySet;
import eu.smartdatalake.simjoin.data.prepared.PreparedStandardSet;
import eu.smartdatalake.simjoin.fuzzysets.FuzzySetCollectionReader;
import eu.smartdatalake.simjoin.sets.TokenSetCollectionReader;

/**
 * Creates a {@link DataESSource} from the given input.
 *
 */
public class DataESSource extends DataSource {
	public String url;
	public String index;
	public String colSetId;
	public String colSetTokens;
	public String colWeights;

	/**
	 * Creates a {@link DataESSource} from a CSV file.
	 * 
	 * @param config The configuration of the input
	 * @param mode   The mode of the input, i.e. Standard or Fuzzy
	 * @return A {@link DataESSource}.
	 */
	public DataESSource(JSONObject config, String mode) {
		super(config, mode, "es");
		
		this.url = String.valueOf(config.get("url"));
		this.index = String.valueOf(config.get("index"));
		this.colSetId = String.valueOf(config.get("colSetId"));
		this.colSetTokens = String.valueOf(config.get("colSetTokens"));
		this.colWeights = String.valueOf(config.get("colWeights"));
	}

	/**
	 * Creates a {@link GroupCollection} from the {@link DataESSource}.
	 * 
	 * @param maxLines Number of lines to parse from the file.
	 * @return A {@link GroupCollection}.
	 */
	public GroupCollection getData(int maxLines) {
		if (mode.equals("fuzzy"))
			return FuzzySetCollectionReader.fromElasticSearch(this, maxLines);
		else
			return TokenSetCollectionReader.fromElasticSearch(this, maxLines);
	}
	
	/**
	 * Prepares the {@link DataSource} by parsing, transforming and creating the index.
	 * 
	 * @param maxLines Number of lines to parse.
	 * @param threshold Threshold for index (only in Standard ThresholdJoin
	 */
	public void prepare(int maxLines, double threshold) {
		if (mode.equals("standard")) {
			prepared = new PreparedStandardSet(TokenSetCollectionReader.fromElasticSearch(this, maxLines), threshold);	
		} else if (mode.equals("fuzzy")) {
			prepared = new PreparedFuzzySet(FuzzySetCollectionReader.fromElasticSearch(this, maxLines));
		}
	}
	
	/**
	 * Whether a weight column has been defined or not.
	 * 
	 * @return A {@link boolean}.
	 */
	public boolean existsWeight() {
		return (!colWeights.equals("") && colWeights != null && colWeights != "null");
	}
	
	/**
	 * Whether an ID column has been defined or not.
	 * 
	 * @return A {@link boolean}.
	 */
	public boolean existsID() {
		return (!colSetId.equals("") && colSetId != null && colSetId != "null");
	}
}
