package eu.smartdatalake.simjoin.simjoinservice.models;

import java.util.ArrayList;
import java.util.List;

import io.swagger.annotations.ApiModelProperty;

public class CatalogOutput {
	@ApiModelProperty(notes = "The list of available configurations")
	public List<String> configs = new ArrayList<String>();
}