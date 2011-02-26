package dremel.dataset;

import static org.junit.Assert.*;

import org.junit.Test;

import dremel.dataset.ColumnMetaData.ColumnType;
import dremel.dataset.ColumnMetaData.EncodingType;
import dremel.dataset.impl.ColumnWriterImpl;
import dremel.dataset.impl.ColumnFileSet;
import dremel.dataset.impl.ColumnReaderImpl;


public class ColumnReaderTest {
		
	public void testIntColumnGeneric(int[] data, boolean[] isNull, byte[] defLevel, byte[] repLevel, byte byteMaxRepLevel, byte maxDefLevel)
	{				
		
		ColumnMetaData columnMetaData= new ColumnMetaData("Links.LinksForward", ColumnType.INT, EncodingType.NONE, "testdata\\LinksForward", byteMaxRepLevel, maxDefLevel);
		
		ColumnWriterImpl columnBuilder = new ColumnWriterImpl(columnMetaData);
		// write data
		for(int i=0; i<data.length; i++)
		{
			columnBuilder.addIntDataTriple(data[i], isNull[i], repLevel[i], defLevel[i]);
		}
		
		columnBuilder.close();
		// verify data
				
		
		ColumnReader columnReader = new ColumnReaderImpl(columnMetaData);		
		
		for(int i=0; i<data.length; i++)
		{			
			assertTrue(columnReader.next());		
			assertEquals(isNull[i], columnReader.isNull());
			assertEquals(columnReader.getRepetitionLevel(), repLevel[i]);
			if(i!= data.length-1)
			{
				assertEquals(columnReader.nextRepetitionLevel(), repLevel[i+1]);
			}else
			{
				assertEquals(columnReader.nextRepetitionLevel(), ColumnReader.EOF_REPETITION_LEVEL);			
			}
		}
		assertFalse(columnReader.next());		
		
	}
	
	@Test
	public void testIntColumn()
	{
		// Example from the paper: Links.Backward column
		// Value  R D 
		// NULL   0 1
		// 10     0 2
		// 30     1 2

		{
		int[]  data = new int[3];
		data[0] =0;
		data[1] =10;
		data[2] =20;
		
		byte[] repLevel = new byte[3];
		repLevel[0] = 0;
		repLevel[1] = 0;
		repLevel[2] = 1;
							
		byte[] defLevel = new byte[3];		
		defLevel[0] = 1;
		defLevel[1] = 2;
		defLevel[2] = 2;

		boolean[] isNull = new boolean[3];
		isNull[0] = true;
		isNull[1] = false;
		isNull[2] = false;
		
		testIntColumnGeneric(data, isNull, defLevel, repLevel,/*max ref level*/ (byte)2 ,/*max def level */(byte)2);
		}
		
		{
			int[]  data = new int[1];
			data[0] =0;
			
			
			byte[] repLevel = new byte[1];
			repLevel[0] = 0;
								
			byte[] defLevel = new byte[1];		
			defLevel[0] = 1;
			
			boolean[] isNull = new boolean[1];
			isNull[0] = true;
			
			testIntColumnGeneric(data, isNull, defLevel, repLevel, /*max ref level*/ (byte)2,/*max def level */(byte)2);
			}
	}

	
}
