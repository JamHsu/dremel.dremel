package dremel.dataset;

/**
 * This interface should be available from the ColumnReader if the data is RLE compressed.
 * @author David.Gruzman
 *
 */
public interface RLESupport {
	/**
	 * returns the number of elements in the column with the same value, repetition and definition level;
	 */
	public int getCountOfRepeatedValues();
	/**
	 * Skips specified number of values, together with their repetition and definition levels
	 * @param valuesToSkip
	 * @return
	 */
	public int skipValues(int valuesToSkip);
	
	// TODO to add predicate aware methods like skipByPredicate. 	
}
