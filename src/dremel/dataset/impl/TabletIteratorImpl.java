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

import java.util.Map;

import dremel.dataset.ColumnReader;
import dremel.dataset.Schema;
import dremel.dataset.TabletIterator;

public class TabletIteratorImpl implements TabletIterator {
	Map<String, ColumnReader> columnsMap;
	byte fetchLevel=0;
	Schema schema = null;
	
	public TabletIteratorImpl(Map<String, ColumnReader> forColumnsMap, Schema forSchema)
	{
		columnsMap = forColumnsMap;
		schema = forSchema;
	}
	
	/* (non-Javadoc)
	 * @see dremel.dataset.impl.TabletIterator#getColumnsMap()
	 */
	@Override
	public Map<String, ColumnReader> getColumnsMap()
	{
		return columnsMap;
	}
	
	/* (non-Javadoc)
	 * @see dremel.dataset.impl.TabletIterator#fetch()
	 */
	@Override
	public boolean fetch()
	{	
		boolean hasMoreSlices = false;
		int nextLevel = 0;
		for(ColumnReader nextReader : columnsMap.values())
		{
			if(nextReader.nextRepetitionLevel() >= fetchLevel)
			{
				boolean isLastInReader = nextReader.next();
				// if at least on reader has more elements, then we have more slices.
				hasMoreSlices = hasMoreSlices || isLastInReader; 
				
			}
			nextLevel = Math.max(nextLevel, nextReader.nextRepetitionLevel());
		}
		return hasMoreSlices;
	}

	@Override
	public byte getFetchLevel() {	
		return fetchLevel;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}				

}
