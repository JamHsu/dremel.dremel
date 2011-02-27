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

public class ColumnFileSet
{
	private String baseFileName;
	private String dataFileName;
	private String defFileName;
	private String repFileName;
	
	public ColumnFileSet(String forBaseFileName)
	{
		baseFileName = forBaseFileName;
		
		dataFileName = baseFileName+"_data.dremel";
		repFileName = baseFileName+"_ref.dremel";
		defFileName = baseFileName+"_def.dremel";
	}
	
	public ColumnFileSet(ColumnFileSet fileSet) {
		// TODO Auto-generated constructor stub
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
	public String getBaseFileName()
	{
		return baseFileName;
	}
			
}