package eu.smartdatalake.simjoin.fuzzysets;

import java.util.List;
import java.util.Set;

public class FuzzySet {

	public FuzzySet(String id, List<Set<String>> elements) {
		super();
		this.id = id;
		this.elements = elements;
	}

	public String id;
	public List<Set<String>> elements;
}