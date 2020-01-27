package eu.smartdatalake.simjoin;

import java.util.ArrayList;

/**
 * Represents a group of elements (e.g., a set of strings).
 *
 * @param <T>
 *            The type of the elements in the group.
 * 
 */
public class Group<T> {
	/**
	 * A unique identifier for the group.
	 */
	public String id;
	/**
	 * The elements comprising the group. should be distinct.
	 */
	public ArrayList<T> elements;
	/**
	 * A value between 0 and 1 assigning a weight to the group.
	 */
	public double weight;
}