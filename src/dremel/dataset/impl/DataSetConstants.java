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

public class DataSetConstants {
	
	// we introduce magic numbers at the beginning of the open dremel column files, to prevent some human errors	
	public static final int REPETITION_COLUMN_MAGIC  = 0xEF01;
	public static final int DEFINITION_COLUMN_MAGIC  = 0xEF02;
	
	public static final int BYTE_COLUMN_MAGIC = 0xEF03;
	public static final int INT_COLUMN_MAGIC  = 0xEF04;

}
