package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class CSVParams {

	@ApiModelProperty(notes = "The path to the csv file.")
	public String file;

	@ApiModelProperty(notes = "The column to use as key.")
	public int colSetId;

	@ApiModelProperty(notes = "The column to use as tokens.")
	public int colSetTokens;

	@ApiModelProperty(notes = "The column to use as weights. (optional)")
	public int colWeights;

	@ApiModelProperty(notes = "The delimiter of the file.")
	public String columnDelimiter;

	@ApiModelProperty(notes = "Whether the file has a header.")
	public boolean header;

	public void addToConfig(JSONObject config) {
		config.put("file", file);
		config.put("colSetId", colSetId);
		config.put("colSetTokens", colSetTokens);
		config.put("columnDelimiter", columnDelimiter);
		config.put("colWeights", colWeights);
		config.put("colWeights", colWeights);
		config.put("header", header);
	}
}