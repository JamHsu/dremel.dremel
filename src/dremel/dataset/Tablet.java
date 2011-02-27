/**
   Copyright 2010, BigDataCraft.Com Ltd.
   David Gruzman

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.Ope
*/
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
