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
package dremel.dataset.impl.encoding.bit;

import java.io.IOException;
import java.io.OutputStream;

import dremel.dataset.impl.encoding.StreamEncoder;



/**
 * Implements RLE encoding scheme. This class overrides InputStream's functionality
 * @author babay
 *
 */
public class BitEncoderImpl extends StreamEncoder {
	
	private static final int MAX_BITS = Integer.SIZE;
	
	int maxBitsWidth = 0;
	int data = 0;
	int numOfUsedBits = 0;
	boolean firstTime = true;
	byte[] bout = new byte[MAX_BITS / Byte.SIZE];
	
	public static BitEncoderImpl instance(OutputStream out, int maxBitsWidth) {
		assert(maxBitsWidth >= 0);
		return new BitEncoderImpl(out, maxBitsWidth);
	}
	
	/**
	 * Constructor
	 */
	private BitEncoderImpl(OutputStream out, int maxBitsWidth) {
		super.setOutputStream(out);
		this.maxBitsWidth = maxBitsWidth;
	}
	
	/**
	 * Encodes the specified byte and writes encoded data to given output stream
	 */
	public void write(int b) {
		if (firstTime) {
			firstTime = false;			
			data = maxBitsWidth; // storing max. bandwidth, we need its value for the decoding operation
			writeEncodedData();
		}
		encode(b);
	}
	
	/**
	 * Flushes this output stream and forces any buffered output bytes to be written out.
	 */
	public void flush() {
		super.flush();
		if (data != 0) {
			writeEncodedData();
			try {
				encodedOut.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Closes this output stream and releases any system resources associated with this stream.
	 */
	public void close() {
		super.close();
		flush();
	}
	
	/**
	 * implements BIT encoding algorithm 
	 * @throws IOException 
	 */
	protected void encode(int b) {
		if (numOfUsedBits + maxBitsWidth > MAX_BITS) {
			writeEncodedData();
		}				
		data |= b << numOfUsedBits;
		numOfUsedBits += maxBitsWidth;
	}
	
	protected void writeEncodedData() {
		try {
			int l = 0;
			for(int i = 0, j = 0; i < MAX_BITS; i += Byte.SIZE, j++) {
				bout[j] = (byte)((data >> i) & 0xff);
			}
			encodedOut.write(bout);
			data = 0;
			numOfUsedBits = 0;			
		} catch (IOException e) {
			throw new RuntimeException("writeEncodedData has failed");
		}
	}
}
