package eu.smartdatalake.simjoin.simjoinservice.models;

import eu.smartdatalake.simjoin.simjoinservice.models.nested.ListPairs;
import io.swagger.annotations.ApiModelProperty;

public class JoinOutput {
	@ApiModelProperty(notes = "The message of creating the join")
	public String message;

	public JoinOutput(String message) {
		this.message = message;
	} 
}