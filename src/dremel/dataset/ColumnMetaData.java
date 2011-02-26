package dremel.dataset;

import dremel.dataset.impl.ColumnFileSet;

public class ColumnMetaData {
	public enum EncodingType {NONE, RLE, BIT, DICTIONARY};
	public enum ColumnType {BYTE, INT, LONG, FLOAT, DOUBLE, STRING, NOT_EXISTING_TYPE};
	
	EncodingType encodingType;
	ColumnFileSet fileSet;
	String columnName;
	ColumnType columnType;
	
	byte maxRepetitionLevel;
	byte maxDefinitionLevel;
	
	public ColumnMetaData(String forColumnName, ColumnType forColumnType, EncodingType forEncodingType, String baseFileName, byte forMaxRepLevel, byte forMaxDefLevel)
	{
		encodingType = forEncodingType;
		fileSet = new ColumnFileSet(baseFileName);
		columnName = forColumnName; 
		columnType = forColumnType;
		
		maxRepetitionLevel = forMaxRepLevel;
		maxDefinitionLevel = forMaxDefLevel;
	}
			
	/**
	 * Copy constructor
	 * @param otherColumnMetaData
	 */
	public ColumnMetaData(ColumnMetaData other) {
		
		this.encodingType = other.encodingType;
		this.fileSet = new ColumnFileSet(other.getFileSet());
		this.columnName = other.columnName; 
		this.columnType = other.columnType;
		
		this.maxRepetitionLevel = other.maxRepetitionLevel;
		this.maxDefinitionLevel = other.maxDefinitionLevel;
	}

	public ColumnFileSet getFileSet() {
		// TODO Auto-generated method stub
		return fileSet;
	}
	
	public ColumnType getColumnType()
	{
		return columnType;
	}

	public byte getMaxDefinitionLevel() {
		return maxDefinitionLevel;
	}
	public byte getMaxRepetitionLevel() {
		return maxRepetitionLevel;
	}

	/**
	 * Replace the fileSet in the given column metadata with the new base file name
	 * @param columnBaseFileName
	 */
	public void setBaseFileName(String columnBaseFileName) {
		fileSet = new ColumnFileSet(columnBaseFileName);		
	}

	public String getColumnName() {		
		return columnName;
	}

	public EncodingType getEncoding() {
		return encodingType;
	}

	
	
	
}
