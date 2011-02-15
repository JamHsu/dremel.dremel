package dremel.dataset;

/**
 * This interface of the ColumnReader. It enable both regular value by value access, and the compression aware access.
 * @author David.Gruzman
 *
 */
public interface ColumnReader {
	public static final int EOF_REPETITION_LEVEL = 0; // value to be returned when nextRepetitionLevel is called for the last element in column
	
	enum ColumnType {BYTE, INT, LONG, FLOAT, DOUBLE, STRING, NOT_EXISTING_TYPE};
	public ColumnType getDataType();
	public boolean isNull();
	public int getRepetitionLevel();
	public int nextRepetitionLevel();
	
	public boolean next();
	
	/**
	 * GetXXXValue methods should be called according to the result of getDataType() method result
	 * @return
	 */
	public int getIntValue();
	public int getLongValue();
	public byte getByteValue();
		
}
