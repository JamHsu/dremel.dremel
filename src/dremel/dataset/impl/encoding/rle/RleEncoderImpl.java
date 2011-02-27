/**
   Copyright 2010, BigDataCraft.Com Ltd.
   Oleg Gibaev

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
package dremel.dataset.impl.encoding.rle;

import java.io.IOException;
import java.io.OutputStream;

import dremel.dataset.impl.encoding.StreamEncoder;



/**
 * Implements RLE encoding scheme. This class overrides InputStream's functionality
 * @author babay
 *
 */
public class RleEncoderImpl extends StreamEncoder {
	
	byte data[] = new byte[2];
	byte lastNeedle = 0;
	int head = 0;
	int startIndex = 1;
	boolean firstTime = true;
	byte repeatedBytes = 0;
	boolean flushed = false;
	
	public static RleEncoderImpl instance(OutputStream out) {
		return new RleEncoderImpl(out);
	}
	
	/**
	 * Constructor
	 */
	private RleEncoderImpl(OutputStream out) {
		super.setOutputStream(out);
	}
	
	/**
	 * Encodes the specified byte and writes encoded data to given output stream
	 */
	public void write(int b) {
		data[head] = (byte)b;
		if (head == data.length - 1) {
			head++;
			encode();
			head = 0;
		}
		else {
			head++;
		}
		flushed = false;
	}
	
	/**
	 * Flushes this output stream and forces any buffered output bytes to be written out.
	 */
	public void flush() {
		super.flush();
		if(!flushed)
		{
			encode();
			writeEncodedTuple();
		}
		flushed = true;
	}

	/**
	 * Closes this output stream and releases any system resources associated with this stream.
	 */
	public void close() {
		super.close();
		flush();
		firstTime = true;		
		head = 0;
	}
	
	/**
	 * implements RLE encoding algorithm. the data is read from the 'data' and written back to the 'out'. 
	 * @throws IOException 
	 */
	protected void encode() {
		
		assert((encodedOut != null));
		
		lastNeedle    = (firstTime) ? data[0] : lastNeedle;
		startIndex    = (firstTime) ? 1 : 0;
		repeatedBytes = (firstTime) ? 1 : repeatedBytes;
		firstTime = false;
		
		for(int i = startIndex; i < head; i++) {
			if (lastNeedle == data[i]) {
				repeatedBytes++;
				continue;
			}
			writeEncodedTuple();
			lastNeedle = data[i];
			repeatedBytes = 1;
		}
	}
	
	protected void writeEncodedTuple() {
		try {
			// writes number of repeated 'head'			
			encodedOut.write(repeatedBytes);
			// writes 'head' itself
			encodedOut.write(lastNeedle);					
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
