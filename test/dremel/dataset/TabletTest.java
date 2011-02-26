package dremel.dataset;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import dremel.dataset.ColumnMetaData.ColumnType;
import dremel.dataset.ColumnMetaData.EncodingType;
import dremel.dataset.impl.ColumnWriterImpl;
import dremel.dataset.impl.SchemaImpl;
import dremel.dataset.impl.TabletBuilderImpl;
import dremel.dataset.impl.TabletImpl;

public class TabletTest {
	
	
	public void buildColumnData(ColumnMetaData columnMetaData)
	{
		ColumnWriterImpl columnBuilder = new ColumnWriterImpl(columnMetaData);
		// write data
		columnBuilder.addIntDataTriple(0, ColumnReader.NULL, (byte)0, (byte)1);
		columnBuilder.addIntDataTriple(10, ColumnReader.NOT_NULL, (byte)0, (byte)2);
		columnBuilder.addIntDataTriple(30, ColumnReader.NOT_NULL, (byte)1, (byte)2);
		
		columnBuilder.close();
	
	}
	
	@Test
	public void singleColumnTabletRoundtripTest()
	{
		// build single column tablet for the input
		ColumnMetaData columnMetaData= new ColumnMetaData("Links.LinksForward", ColumnType.INT, EncodingType.NONE, "testdata\\LinksForward", (byte)1, (byte)2);
		buildColumnData(columnMetaData);
		
		Map<String, ColumnMetaData> columns = new HashMap<String, ColumnMetaData>();
		columns.put(columnMetaData.getColumnName(), columnMetaData );
		
		SchemaImpl schema = new SchemaImpl();
		schema.addColumnMetaData(columnMetaData);
		
		Tablet tablet = new TabletImpl(schema);
						
		TabletIterator tabletIterator = tablet.getIterator();		
		
		// create output tablet
		TabletBuilderImpl outputTablet = new TabletBuilderImpl();
		outputTablet.buildTablet("testdata\\testOutput", tabletIterator);
		// to make fetch / insert loop.
		while(tabletIterator.fetch())
		{
			outputTablet.pushSlice(tabletIterator.getFetchLevel());// pass fetch level as select level because it is NULL-query
		}
		outputTablet.close();		
		// create input tablet over the output
		Schema resultSchema = outputTablet.getSchema();
		Tablet resultTablet = new TabletImpl(resultSchema);
		// compare input and output data
		Tablet originalTablet = new TabletImpl(schema);
		assertTrue(compareTablets(resultTablet, originalTablet));
	}

	private boolean compareTablets(Tablet firstTablet, Tablet secondTablet) {
		boolean tabletEquals = true;
		
		if(firstTablet.getSchema().getColumnsMetaData().keySet().size() != secondTablet.getSchema().getColumnsMetaData().keySet().size())
		{
			return false;
		}
		
		for(String columnName : firstTablet.getSchema().getColumnsMetaData().keySet())
		{
			ColumnReader firstColumnReader = firstTablet.getColumns().get(columnName);
			ColumnReader secondColumnReader = secondTablet.getColumns().get(columnName);
			
			tabletEquals = tabletEquals && compareColumnReaders(firstColumnReader, secondColumnReader);
		}
		
		return tabletEquals;
	}

	private boolean compareColumnReaders(ColumnReader firstReader,
			ColumnReader secondReader) {
		boolean columnsEquals = true;
		
		if(firstReader.getDataType() != secondReader.getDataType())
		{
			return false;
		}
		
		while(firstReader.next())
		{
			if(!secondReader.next())
			{
				// we have more values in the first column
				return false;
			}
			
			switch(firstReader.getDataType())
			{
			case INT: 
				columnsEquals = columnsEquals && (firstReader.getIntValue() == secondReader.getIntValue());
				break;
			default: throw new RuntimeException("Unsupported data type");
			}
		}
		
		if(secondReader.next())
		{
			// we have more values in the second column
			return false;
		}
		
		
		return columnsEquals;
	}
}
