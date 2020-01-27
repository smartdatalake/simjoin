package eu.smartdatalake.simjoin;

import java.util.ArrayList;

/**
 * Represents a collection of groups.
 *
 * @param <T>
 *            The type of the elements in the groups.
 * 
 */
public class GroupCollection<T> {
	/**
	 * The groups comprising the collection.
	 */
	public ArrayList<Group<T>> groups;
}