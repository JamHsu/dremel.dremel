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

import java.util.Map;

/**
 * In this stage it is not generic implementation but set of methods we need now for the dataset
 * @author David.Gruzman
 *
 */
public interface Schema {		
	public ColumnMetaData getColumnMetaData(String columnName);
	/**
	 * @return map from the column name to their meta data
	 */
	public Map<String, ColumnMetaData> getColumnsMetaData();
}
