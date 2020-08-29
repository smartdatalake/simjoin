package eu.smartdatalake.simjoin.simjoinservice.models;

import eu.smartdatalake.simjoin.simjoinservice.models.nested.Params;
import io.swagger.annotations.ApiModelProperty;

public class JoinInput {
	@ApiModelProperty(notes = "The join parameters")
	public Params params;
}