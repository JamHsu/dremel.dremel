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
package dremel.dataset;

/**
 * This interface of the ColumnReader. It enable both regular value by value access, and the compression aware access.
 * @author David.Gruzman
 *
 */
public interface ColumnReader {
	public static final int EOF_REPETITION_LEVEL = 0; // value to be returned when nextRepetitionLevel is called for the last element in column
	public static final boolean NOT_NULL = false;
	public static final boolean NULL = true;
		
	public ColumnMetaData.ColumnType getDataType();
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
	public String getStringValue();
		
}
