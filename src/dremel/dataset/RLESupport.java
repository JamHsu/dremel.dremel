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
 * This interface should be available from the ColumnReader if the data is RLE compressed. Not intended to be used
 * in Metaxa.
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
