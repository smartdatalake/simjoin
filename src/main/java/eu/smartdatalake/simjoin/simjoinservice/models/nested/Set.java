package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class Set {

	@ApiModelProperty(notes = "ID of set.")
	public String id;

	@ApiModelProperty(notes = "Tokens of set.")
	public String set;

	@ApiModelProperty(notes = "Weight of set.")
	public Double weight;

	public JSONObject toJSON() {
		JSONObject j = new JSONObject();
		j.put("id", id);
		j.put("set", set);
		if (weight != null)
			j.put("weight", weight);
		return j;
	}
}