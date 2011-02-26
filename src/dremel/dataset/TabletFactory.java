package dremel.dataset;

import java.util.Map;

/**
 * This class contains methods for tablet creation. Currently it contain method to create the tablet 
 * located on the file system. 
 * @author David.Gruzman
 *
 */
public class TabletFactory {
	/**
	 * Tablet TabletMetaData contains information about the tablet column's location.
	 * its columns an
	 * @author David.Gruzman
	 *
	 */
	public static class TabletLocation
	{
		public Map<String, String> columnLocations;
	}
	
	static Tablet createTableFromDescription(TabletLocation location)
	{
		return null;
	}
}
