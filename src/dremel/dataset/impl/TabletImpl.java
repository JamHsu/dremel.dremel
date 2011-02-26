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
package dremel.dataset.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dremel.dataset.ColumnReader;
import dremel.dataset.Schema;
import dremel.dataset.Tablet;
import dremel.dataset.TabletIterator;

public class TabletImpl implements Tablet {

	Schema schema;
	Map<String, ColumnReader> columnReaders = null;
	
	public TabletImpl(Schema forSchema) {
		schema = forSchema;
		initReaders();
	}

	private void initReaders() {
		
		columnReaders = new HashMap<String, ColumnReader>();
		
		for(String columnName : schema.getColumnsMetaData().keySet())
		{
			ColumnReader reader = new ColumnReaderImpl(schema.getColumnMetaData(columnName));
			columnReaders.put(columnName, reader);
		}
		
	}

	@Override
	public TabletIterator getIterator() {
		TabletIterator tabletIter = new TabletIteratorImpl(columnReaders, schema);
		return tabletIter;
	}

	@Override
	public TabletIterator getProjectionIterator(List<String> columnsInProjection) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Schema getSchema() {
		
		return schema;
	}

	@Override
	public Map<String, ColumnReader> getColumns() {
		return columnReaders;
	}

}
