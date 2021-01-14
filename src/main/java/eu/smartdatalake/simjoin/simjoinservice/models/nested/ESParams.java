package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class ESParams {

	@ApiModelProperty(notes = "URL of the ES database.")
	public String url;
	
	@ApiModelProperty(notes = "Index to search.")
	public String index;
	
	
	@ApiModelProperty(notes = "Name of the Key Column.")
	public String colSetId;
	
	@ApiModelProperty(notes = "Name of the Tokens Column.")
	public String colSetTokens;
	
	@ApiModelProperty(notes = "Name of the Weights Column (Optional)")
	public String colWeights;
	
	public void addToConfig(JSONObject config) {
		config.put("url", url);
		config.put("index", index);
		config.put("colSetId", colSetId);
		config.put("colSetTokens", colSetTokens);
		config.put("colWeights", colWeights);
	}
}