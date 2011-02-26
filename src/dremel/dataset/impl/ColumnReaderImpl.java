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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

import dremel.dataset.ColumnMetaData;
import dremel.dataset.ColumnMetaData.ColumnType;
import dremel.dataset.impl.encoding.EncodingFactory;
import dremel.dataset.impl.encoding.StreamDecoder;
import dremel.dataset.ColumnReader;

/**
 * It is basic implementation of the ColumnReader interface. The column is represented by three files:
 * file with data, file with repetition levels and file with definition levels 
 * @author David.Gruzman
 *
 */
public class ColumnReaderImpl implements ColumnReader {

	ColumnMetaData columnMetaData = null; 

	
	DataInputStream dataInput;
	DataInputStream  definitionInput;
	DataInputStream  repetitionInput;
		
		
	//	
	int currentIntValue;
	byte currentByteValue;
	
	boolean currentIsNull;
	
	byte currentRepetitionLevel;
	byte nextRepetitionLevel;
	
	
	boolean isNextCalled = false;
	transient byte[]singleByteArray = new byte[1];

		
	/**
	 * Constructs the ColumnReaderImpl over set of files.
	 * @param forColumnType - expected type of the column. It will be verified against the magic bytes of the file
	 * @param forDataFileName - name of the file with column data
	 * @param forRepetitionFileName - name of the file with repetition level data.
	 * @param forDefinitionFileName - name of the file with definition level data
	 * @param maxDefinitionLevel - maximum definition level (from the Dremel paper) of the column. It is needed to infer null values;
	 */
	public ColumnReaderImpl (ColumnMetaData forColumnMetaData)
	{
		columnMetaData = forColumnMetaData;
		
		openFiles();
	}

	/**
	 * This method open definition, repetition and data files, and check their magic numbers
	 */
	private void openFiles() {
				
		int dataFileMagic = getColumnMagicBytes(columnMetaData.getColumnType());
		
		ColumnFileSet columnFileSet = columnMetaData.getFileSet();
		
		dataInput 		 = openColumnFile(columnFileSet.getDataFileName(), dataFileMagic, columnMetaData);
		definitionInput  = openColumnFile(columnFileSet.getDefFileName(), DataSetConstants.DEFINITION_COLUMN_MAGIC, columnMetaData);
		repetitionInput  = openColumnFile(columnFileSet.getRepFileName(), DataSetConstants.REPETITION_COLUMN_MAGIC, columnMetaData);

	}
	/**
	 * Opens given file and verify that its magic bytes match the expectations
	 * @param fileName
	 * @param expectedFileMagic
	 * @return
	 */
	private static DataInputStream openColumnFile(String fileName,
			int expectedFileMagic, ColumnMetaData columnMetaData) {
		try {			
			DataInputStream resultStream = new DataInputStream(new FileInputStream(fileName));
			
			int presentMagicNumber = resultStream.readInt();
			if(presentMagicNumber != expectedFileMagic)
			{
				throw new RuntimeException("Incorrect type (by the magic byte analysing) in the file "+fileName+" expected file type is " + getTypeNameByMagic(expectedFileMagic)+ " while the actual is "+getTypeNameByMagic(presentMagicNumber));
			}
			
			//TODO chain the appropriate decoder
			if(columnMetaData.getEncoding() != ColumnMetaData.EncodingType.NONE)
			{
				StreamDecoder decoder = EncodingFactory.getDecoder(columnMetaData, resultStream);
				resultStream = new DataInputStream(decoder);
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
		return columnMetaData.getColumnType();
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
			throw new RuntimeException("Reading defenition level from the file "+columnMetaData.getFileSet().getDefFileName() + "failed", e);
	}	
		
		// read repetition level
		readRepetitionLevel();
		
		if(nextDefLevel == columnMetaData.getMaxDefinitionLevel())
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
		if(columnMetaData.getColumnType() == ColumnType.INT)
		{
			currentIntValue = dataInput.readInt();			
			return;
		}
		
		
		if(columnMetaData.getColumnType() == ColumnType.BYTE)
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
				throw new RuntimeException("Read repetition level from the file "+columnMetaData.getFileSet().getRepFileName() + " failed "+e);
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
			throw new RuntimeException("Read repetition level from the file "+columnMetaData.getFileSet().getRepFileName() + " failed "+e);
		}
	}

	@Override
	public String getStringValue() {
		throw new RuntimeException("Not implemented");
	}

}
