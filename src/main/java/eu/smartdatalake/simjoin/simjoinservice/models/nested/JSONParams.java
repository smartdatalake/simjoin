package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import io.swagger.annotations.ApiModelProperty;

public class JSONParams {

	@ApiModelProperty(notes = "List of sets.")
	public Set[] values;
	
	public void addToConfig(JSONObject config) {

		JSONArray jr = new JSONArray();
		for (Set v: values) {
			jr.add(v.toJSON());
		}
		config.put("values", jr);
	}
	
}