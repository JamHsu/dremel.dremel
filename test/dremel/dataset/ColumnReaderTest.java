package dremel.dataset;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

import dremel.dataset.ColumnReader.ColumnType;
import dremel.dataset.impl.ColumnReaderImpl;
import dremel.dataset.impl.DataSetConstants;

public class ColumnReaderTest {
	
	
	private static class ColumnBuilder
	{
		DataOutputStream dataOutput;
		DataOutputStream repOutput;
		DataOutputStream defOutput;
		
		public ColumnBuilder(ColumnReaderImpl.ColumnFileSet fileSet, ColumnReader.ColumnType columnType)
		{
			try {
							
			dataOutput = new DataOutputStream(new FileOutputStream(new File(fileSet.getDataFileName())));
			repOutput = new DataOutputStream(new FileOutputStream(new File(fileSet.getRepFileName())));
			defOutput = new DataOutputStream(new FileOutputStream(new File(fileSet.getDefFileName())));
			
			int dataMagicBytes = getMagicByColumnType(columnType);
			
			dataOutput.writeInt(dataMagicBytes);
			repOutput.writeInt(DataSetConstants.REPETITION_COLUMN_MAGIC);
			defOutput.writeInt(DataSetConstants.DEFINITION_COLUMN_MAGIC);
				
			} catch (IOException e) {
				throw new RuntimeException("Open output file failed ", e);
			}
			
		}
		
		private int getMagicByColumnType(ColumnType columnType) {
			
			switch(columnType)
			{
				case BYTE : return DataSetConstants.BYTE_COLUMN_MAGIC;
				case INT : return DataSetConstants.INT_COLUMN_MAGIC;
				default : throw new RuntimeException("unexpected column type "+ columnType); 
			}			
		}

		
		public void addIntDataTriple(int data, boolean isNull, byte repLevel, byte defLevel)
		{
			try {

				if(!isNull)
				{ // NULL values are not written to the storage. They are deduced from the definition level during the read
					dataOutput.writeInt(data);
				}			
				repOutput.writeByte(repLevel);
				defOutput.writeByte(defLevel);
			
				} catch (IOException e) {
					throw new RuntimeException("Writing data to the column failed", e);
			}
		}
		
	
		private void close() {		
			try {
				dataOutput.close();		
				repOutput.close();
				defOutput.close();		
			} catch (IOException e) {
				throw new RuntimeException("Closing data streams failed ",e);
			}
}
	
	}
	
	public void testIntColumnGeneric(int[] data, boolean[] isNull, byte[] defLevel, byte[] repLevel, byte maxDefLevel)
	{		
		ColumnReaderImpl.ColumnFileSet fileSet = new ColumnReaderImpl.ColumnFileSet("testdata\\LinksForward");
		ColumnBuilder columnBuilder = new ColumnBuilder(fileSet, ColumnType.INT);
		// write data
		for(int i=0; i<data.length; i++)
		{
			columnBuilder.addIntDataTriple(data[i], isNull[i], repLevel[i], defLevel[i]);
		}
		
		columnBuilder.close();
		// verify data
		
		ColumnReader columnReader = new ColumnReaderImpl(ColumnType.INT, fileSet, maxDefLevel);
		
		
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
		
		testIntColumnGeneric(data, isNull, defLevel, repLevel, /*max def level */(byte)2);
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
			
			testIntColumnGeneric(data, isNull, defLevel, repLevel, /*max def level */(byte)2);
			}
	}

	
}
