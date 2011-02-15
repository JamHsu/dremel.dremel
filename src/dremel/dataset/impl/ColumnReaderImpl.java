package dremel.dataset.impl;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

import dremel.dataset.ColumnReader;
/**
 * It is basic implementation of the ColumnReader interface. The column is represented by three files:
 * file with data, file with repetition levels and file with definition levels 
 * @author David.Gruzman
 *
 */
public class ColumnReaderImpl implements ColumnReader {


	ColumnType columnType;
	ColumnFileSet columnFileSet;
	
	DataInputStream dataInput;
	DataInputStream  definitionInput;
	DataInputStream  repetitionInput;
		
	
	byte maximumDefinitionLevel;
	//	
	int currentIntValue;
	byte currentByteValue;
	
	boolean currentIsNull;
	
	byte currentRepetitionLevel;
	byte nextRepetitionLevel;
	
	
	boolean isNextCalled = false;
	transient byte[]singleByteArray = new byte[1];

	
	
	//---------------------------------- constants -----------------------------------

	
	
	
	/**
	 * Constructs the ColumnReaderImpl over set of files.
	 * @param forColumnType - expected type of the column. It will be verified against the magic bytes of the file
	 * @param forDataFileName - name of the file with column data
	 * @param forRepetitionFileName - name of the file with repetition level data.
	 * @param forDefinitionFileName - name of the file with definition level data
	 * @param maxDefinitionLevel - maximum definition level (from the Dremel paper) of the column. It is needed to infer null values;
	 */
	public ColumnReaderImpl (ColumnType forColumnType, ColumnFileSet fileSet, byte maxDefinitionLevel)
	{
		columnType = forColumnType;
		columnFileSet = fileSet;
		maximumDefinitionLevel = maxDefinitionLevel;
		openFiles();
	}

	/**
	 * This method open definition, repetition and data files, and check their magic numbers
	 */
	private void openFiles() {
				
		int dataFileMagic = getColumnMagicBytes(columnType);
		
		dataInput 		 = openColumnFile(columnFileSet.getDataFileName(), dataFileMagic);
		definitionInput  = openColumnFile(columnFileSet.getDefFileName(), DataSetConstants.DEFINITION_COLUMN_MAGIC);
		repetitionInput  = openColumnFile(columnFileSet.getRepFileName(), DataSetConstants.REPETITION_COLUMN_MAGIC);

	}
	/**
	 * Opens given file and verify that its magic bytes match the expectations
	 * @param fileName
	 * @param expectedFileMagic
	 * @return
	 */
	private static DataInputStream openColumnFile(String fileName,
			int expectedFileMagic) {
		try {
			DataInputStream resultStream = new DataInputStream(new FileInputStream(fileName));
			
			int presentMagicNumber = resultStream.readInt();
			if(presentMagicNumber != expectedFileMagic)
			{
				throw new RuntimeException("Incorrect type (by the magic byte analysing) in the file "+fileName+" expected file type is " + getTypeNameByMagic(expectedFileMagic)+ " while the actual is "+getTypeNameByMagic(presentMagicNumber));
			}
			
			return resultStream;
		} catch (IOException e) {
			throw new RuntimeException(" opening file "+fileName+ " failed ", e);
		}
	}

	private static String getTypeNameByMagic(int columnFileMagic) {
		switch (columnFileMagic)
		{
			case DataSetConstants.REPETITION_COLUMN_MAGIC: return "REPETITION_COLUMN"; 
			case DataSetConstants.DEFINITION_COLUMN_MAGIC: return "DEFINITION_COLUMN";
			case DataSetConstants.BYTE_COLUMN_MAGIC: return "BYTE_COLUMN";
			case DataSetConstants.INT_COLUMN_MAGIC: return "INT_COLUMN";
			default: return "unknown type with magic"+columnFileMagic;
		}
		
	}

	private static int getColumnMagicBytes(ColumnType expectedColumnType) {
		switch(expectedColumnType)
		{
			case BYTE: return DataSetConstants.BYTE_COLUMN_MAGIC;
			case INT: return  DataSetConstants.INT_COLUMN_MAGIC;
			default: throw new RuntimeException("Unexpected column type" + expectedColumnType);
		}		
	}

	@Override
	public ColumnType getDataType() {
		return columnType;
	}

	@Override
	public boolean isNull() {
		return currentIsNull;
	}

	@Override
	public int getRepetitionLevel() {
		return currentRepetitionLevel;
	}

	/**
	 * Return the repetition level of the next entry. If current entry is the last, then next repetition level will be EOF_REPETITION_LEVEL.
	 */
	@Override
	public int nextRepetitionLevel() {
		return nextRepetitionLevel;
	}

	@Override
	public int getIntValue() {		
		return currentIntValue;
	}

	@Override
	public int getLongValue() {
		throw new RuntimeException(" not implemented yet");		
	}

	@Override
	public byte getByteValue() {		
		return currentByteValue;	
	}
	

	@Override
	public boolean next() {
				
		// read definition level		
		byte nextDefLevel;
		
		try 
		{
			nextDefLevel = definitionInput.readByte();
		} 
		catch (EOFException e) {
			return false;	
		}
		catch (IOException e) {
			throw new RuntimeException("Reading defenition level from the file "+columnFileSet.getDefFileName() + "failed", e);
	}	
		
		// read repetition level
		readRepetitionLevel();
		
		if(nextDefLevel == maximumDefinitionLevel)
		{
			currentIsNull=false;
			// read typed value;
			readNextDataValue();
			
		}else
		{
			currentIsNull=true;
		}
		
		// it is important to make it last call in the next method!! Since some methods called within next() use this variable and want to know that call is a first time
		isNextCalled = true; // before next is called  ColumnReader can not return values. Also used for the nextRepetitionLevel reading for the first time
		
		return true;
	}

	private void readNextDataValue() {
		
		try {
		if(columnType == ColumnType.INT)
		{
			currentIntValue = dataInput.readInt();			
			return;
		}
		
		
		if(columnType == ColumnType.BYTE)
		{
			currentByteValue = dataInput.readByte();
			return;
		}
		
		} catch (IOException e) {
			throw new RuntimeException("read value failed", e);		}
		
		throw new RuntimeException("Unsupported data type");
		
	}

	private void readRepetitionLevel() {
		if(isNextCalled)
		{
			currentRepetitionLevel = nextRepetitionLevel;			
			readNextRepetitionLevel();
		}else
		{ // first time call
			try {
				int res = repetitionInput.read(singleByteArray);
				if(res ==-1)
				{
					throw new RuntimeException("At least one rep level value should exist if we get there");
				}
				currentRepetitionLevel = singleByteArray[0];
				readNextRepetitionLevel();
				
			} catch (IOException e) {
				throw new RuntimeException("Read repetition level from the file "+columnFileSet.getRepFileName() + " failed "+e);
			}	
		}
	}

	private void readNextRepetitionLevel() {
		try {
			int res = repetitionInput.read(singleByteArray);
			if(res != -1)
			{
				nextRepetitionLevel = singleByteArray[0];
			}else
			{ // there no more data in the repetitionInput, so we put predefined value to the nextRepetition
				nextRepetitionLevel = EOF_REPETITION_LEVEL;
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Read repetition level from the file "+columnFileSet.getRepFileName() + " failed "+e);
		}
	}

	public static class ColumnFileSet
	{
		private String dataFileName;
		private String defFileName;
		private String repFileName;
		
		public ColumnFileSet(String baseFileName)
		{
			dataFileName = baseFileName+"_data.dremel";
			repFileName = baseFileName+"_ref.dremel";
			defFileName = baseFileName+"_def.dremel";
		}
		
		public String getDataFileName()
		{
			return dataFileName;
		}
		public String getDefFileName()
		{
			return defFileName;
		}
		public String getRepFileName()
		{
			return repFileName;
		}
				
	}

}
