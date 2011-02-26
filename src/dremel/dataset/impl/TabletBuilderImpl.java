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
import java.util.Map;

import dremel.dataset.ColumnMetaData;
import dremel.dataset.ColumnReader;
import dremel.dataset.ColumnWriter;
import dremel.dataset.Schema;
import dremel.dataset.TabletIterator;

/**
 * This class is responsible for building tablet from the given tablet iterator
 * @author David.Gruzman
 *
 */
public class TabletBuilderImpl {
	String tabletName = null;	
	Map<String, ColumnWriter > columnWriters = null;
	TabletIterator inputIterator;
	Schema resultSchema = null;
	
	public void buildTablet(String forTabletName, TabletIterator forInputTabletIterator)
	{
		tabletName = forTabletName;
		inputIterator = forInputTabletIterator;
		initColumnWriters();
	}
	
	public Schema getSchema()
	{
		return resultSchema;
	}
	
	private static Schema buildOutputSchemaFromInputSchema(Schema inputSchema, String outputTabletName)
	{
		SchemaImpl localResultShema = new SchemaImpl();
		
		// iterate over column readers
	//	Map<String, ColumnReader > inputColumns = inputIterator.getColumnsMap();		
		
		for(String columnName : inputSchema.getColumnsMetaData().keySet())
		{
			String columnBaseFileName = generateColumnName(outputTabletName, columnName);
			
			// get input column meta data, and replace the file name, with name for the tablet under construction
			ColumnMetaData inputColumnMetaData = inputSchema.getColumnMetaData(columnName);
			// make a copy
			ColumnMetaData outputColumnMetaData = new ColumnMetaData(inputColumnMetaData);
			// replace the name
			outputColumnMetaData.setBaseFileName(columnBaseFileName);
			
			localResultShema.addColumnMetaData(outputColumnMetaData);						
		}	
		
		return localResultShema;
	}
	/**
	 * This method create column writer for each column in the inputIterator, and store it in the columnWriters map
	 */	
	private void initColumnWriters() {
		
		columnWriters = new HashMap<String, ColumnWriter >();
		resultSchema = buildOutputSchemaFromInputSchema(inputIterator.getSchema(), tabletName);
						
		for(ColumnMetaData nextColumnMetaData : resultSchema.getColumnsMetaData().values())
		{
					
			ColumnWriter columnWriter = new ColumnWriterImpl(nextColumnMetaData);
			columnWriters.put(nextColumnMetaData.getColumnName(), columnWriter);			
		}					
		
	}

	/**
	 * Builds the name of the column file , for the given column and given tablet name
	 * @param baseFileName2
	 * @param columnName
	 * @return
	 */
	private static String generateColumnName(String baseFileName, String columnName) {
		return baseFileName + "_"+columnName;
	}
	
	
	
	public void pushSlice(byte selectLevel)
	{
		Schema schema = inputIterator.getSchema();
		// for each column in the tablet iterator
		for(String columnName : inputIterator.getColumnsMap().keySet())		
		{
			assert(schema.getColumnsMetaData().containsKey(columnName));
			// if max rep level of the column is more or equal to the select level - take the value
			if(schema.getColumnMetaData(columnName).getMaxRepetitionLevel()>=selectLevel)
			{
				ColumnReader reader = inputIterator.getColumnsMap().get(columnName);
				ColumnWriter writer = columnWriters.get(columnName);
				assert(writer != null);
				transferValue(columnName, reader, writer, selectLevel);
			}
		}		
	}
	/**
	 * This method should read the value from the given reader, deduce definition level, and
	 * write it to the writer.
	 * Type of the value should be taken into account.
	 * selectLevel should be used as repetition level.
	 * @param reader
	 * @param writer
	 * @param selectLevel
	 */
	private void transferValue(String columnName, ColumnReader reader, ColumnWriter writer,
			byte selectLevel) {
		byte definitionLevel = inferDefenitionLevel(columnName, reader);
		if(reader.isNull())
		{ // handle the case when 
			writer.setNullValue(selectLevel, definitionLevel);
			return;
		}
		
		switch(reader.getDataType())
		{
		case INT:
			writer.addIntDataTriple(reader.getIntValue(), ColumnReader.NOT_NULL, selectLevel, definitionLevel);
			break;
		default: throw new RuntimeException("Time not supported" +reader.getDataType());
		}
		
	}
	
	private byte inferDefenitionLevel(String columnName, ColumnReader reader) {
		
		if(!reader.isNull())
		{
			return inputIterator.getSchema().getColumnMetaData(columnName).getMaxDefinitionLevel();
		}
		//TODO exact algorithm to deduce the definition level
		
		return 0;
	}
	
	/**
	 * Must be closed after no more calls to the pushSlice are expected
	 * Technically - closes all underlying columns with thier respective fiels or other streams
	 */
	public void close() {
		for(ColumnWriter nextWriter : columnWriters.values())
		{
			nextWriter.close();
		}
		
	}
	

}
