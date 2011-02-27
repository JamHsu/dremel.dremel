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

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.io.InputStream;

import dremel.dataset.impl.encoding.StreamDecoder;


/**
 * Implements Bit decoding scheme
 * @author babay
 *
 */
public class BitDecoderImpl extends StreamDecoder {

	private static final int MAX_BITS = Integer.SIZE;
	
	byte needle = 0;
	int availableData = 1;
	int data = 0;
	int needleBitWidth = 0;
	int packedNeedlesPerBundle = 0;
	boolean firstTime = true;
	byte[] bin = new byte[MAX_BITS / Byte.SIZE];
	
	public static BitDecoderImpl instance(InputStream encodedIn) {
		assert(encodedIn != null);
		return new BitDecoderImpl(encodedIn);
	}
	
	/**
	 * Constructor
	 */
	private BitDecoderImpl(InputStream encodedIn) {
		super.setInStream(encodedIn);
	}

	/**
	 * Returns the number of bytes that can be read (or skipped over) from this input stream without blocking by the next caller of a method for this input stream.
	 * Overridden method
	 */
	public int available() {
		try {
			if ((encodedIn.available() == 0 && data == 0) || (error)) {
				availableData = 0;
			}
		} catch (IOException e) {
			error = true;
			availableData = 0;
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return availableData;
	}
	
	/**
	 * Closes this input stream and releases any system resources associated with the stream.
	 * Overridden method
	 */
	public void close() {
		super.close();
	}
	
	/**
	 * Reads the next needle from the raw input stream.
	 * Implemented method 
	 */
	public int read() throws IOException {
		checkStreamsDataAvailability();
		if (firstTime) {
			firstTime = false;
			// read bits width
			try {
				for(int i = 0; i < MAX_BITS; i += Byte.SIZE) {
					byte b = (byte)encodedIn.read();
					needleBitWidth |= (b << i);
				}				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				error = true;
				e.printStackTrace();
				return 0;
			}
			if (needleBitWidth == -1) {
				error = true;
			}
		}
		if (data == 0) {
			try {
				for(int i = 0; i < MAX_BITS; i += Byte.SIZE) {
					int b = (int)encodedIn.read();
					data |= (b << i);
				}
			} catch (IOException e) {
				error = true;
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 0;
			}
			if (data == -1) {
				error = true;
			}
		}
		decode();
		return needle;
	}
	
	/**
	 * implements Bit decoding algorithm 
	 * @throws IOException 
	 */
	protected void decode() {
		needle = (byte)(data & ((1 << needleBitWidth) - 1));
		data >>= needleBitWidth;
	}
	
}
