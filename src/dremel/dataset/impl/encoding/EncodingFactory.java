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
package dremel.dataset.impl.encoding;

import java.io.InputStream;
import java.io.OutputStream;

import dremel.dataset.ColumnMetaData;
import dremel.dataset.impl.encoding.rle.RleDecoderImpl;
import dremel.dataset.impl.encoding.rle.RleEncoderImpl;

public class EncodingFactory {
	public static StreamEncoder getEncoder(ColumnMetaData columnMetaData, OutputStream outStream)
	{
		switch(columnMetaData.getEncoding())
		{
			case RLE: return RleEncoderImpl.instance(outStream);
			// TODO - to implement notion of the column statistics for the proper use of the bits encoding
			//case BIT: return BitEncoderImpl.instance(outStream, inferBitsWith(columnMetaData.getMaxIntValue())); break;
			
			default: throw new RuntimeException("Not supported encoding type"+columnMetaData.getEncoding());		
		}
	}
	
	public static StreamDecoder getDecoder(ColumnMetaData columnMetaData, InputStream inputStream)
	{
		switch(columnMetaData.getEncoding())
		{
			case RLE: return RleDecoderImpl.instance(inputStream); 
			default: throw new RuntimeException("Not supported encoding type"+columnMetaData.getEncoding());		
		}
	}
}
