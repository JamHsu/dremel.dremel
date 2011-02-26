package dremel.dataset;

import java.util.List;
import java.util.Map;

/**
 * This class represents the interface to the tablet. The main user of the tablet interface is tableton - which performs
 * the query execution over the given tablet.
 * @author David.Gruzman
 *
 */
public interface Tablet {
	
	/**
	 * return iteration capable to produce Slices of the tablet.
	 * @return
	 */
	public TabletIterator getIterator();
	/**
	 * Return iterator over the subset of the tablet column
	 * @param columnsInProjection
	 * @return
	 */
	public TabletIterator getProjectionIterator(List<String> columnsInProjection);
	public Schema getSchema();
	// return the map from the column names to the column readers
	public Map<String, ColumnReader> getColumns();
}
