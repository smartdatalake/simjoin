package eu.smartdatalake.simjoin.simjoinservice.models;

import org.json.simple.JSONObject;

import eu.smartdatalake.simjoin.simjoinservice.models.nested.JoinParams;
import io.swagger.annotations.ApiModelProperty;

public class JoinInput {
	@ApiModelProperty(notes = "The number of results to return")
	public int limit;

	@ApiModelProperty(notes = "The join parameters")
	public JoinParams params;
	
	public JSONObject toJSON() {
		JSONObject config = new JSONObject();
		params.addToConfig(config);
		return config;
	}
}