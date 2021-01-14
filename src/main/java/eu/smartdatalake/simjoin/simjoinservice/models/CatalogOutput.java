package eu.smartdatalake.simjoin.simjoinservice.models;

import java.util.List;

import io.swagger.annotations.ApiModelProperty;

public class CatalogOutput {
	@ApiModelProperty(notes = "The list of available DataSources.")
	public List<String> dataSources;
	
	public CatalogOutput(List<String> datasources) {
		this.dataSources = datasources;
	}
}