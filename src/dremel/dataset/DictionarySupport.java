package dremel.dataset;

import java.util.Map;

public interface DictionarySupport {
	//-------------Methods for the dictionary aware access
	/**
	 * Return the dictionary for this column if it is dictionary compressed. 
	 * It method should not be called if the column is not dictionary compressed. 
	 */
	public Map<Integer, String> getStringDictionary();

}
