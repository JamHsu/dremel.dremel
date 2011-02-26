package dremel.dataset.impl;

import java.util.HashMap;
import java.util.Map;

import dremel.dataset.ColumnMetaData;
import dremel.dataset.Schema;

public class SchemaImpl implements Schema {

	Map<String, ColumnMetaData> columnsMetaData = new HashMap<String, ColumnMetaData>();
	
	public SchemaImpl()
	{
		
	}
	
	public void addColumnMetaData(ColumnMetaData newColumn)
	{
		if(columnsMetaData.containsKey(newColumn.getColumnName()))
		{
			throw new RuntimeException("Column with name "+newColumn.getColumnName() +"Already exists in the schema");
		}
		columnsMetaData.put(newColumn.getColumnName(), newColumn);
	}
	
	@Override
	public ColumnMetaData getColumnMetaData(String columnName) {
		if(!columnsMetaData.containsKey(columnName))
		{
			throw new RuntimeException("Column with name "+columnName +" is not exists in the schema");
		}
		return columnsMetaData.get(columnName);
	}

	@Override
	public Map<String, ColumnMetaData> getColumnsMetaData() {		
		return columnsMetaData;
	}

}
