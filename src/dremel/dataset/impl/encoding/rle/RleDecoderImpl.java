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
import java.io.InputStream;
import java.io.OutputStream;

import dremel.dataset.impl.encoding.StreamDecoder;


/**
 * Implements RLE decoding scheme
 * @author babay
 *
 */
public class RleDecoderImpl extends StreamDecoder {
	
	byte data[] = new byte[2];
	int readBytes = 0;
	int nextRepeater = 0;
	int nextNeedleIdxOffset = 1;
	int availableData = 1;
	byte repeatedNeedles = 0;
	byte needle = 0;
	
	public static RleDecoderImpl instance(InputStream encodedIn) {
		assert(encodedIn != null);
		return new RleDecoderImpl(encodedIn);
	}
	
	/**
	 * Constructor
	 */
	private RleDecoderImpl(InputStream encodedIn) {
		super.setInStream(encodedIn);
	}

	/**
	 * Returns the number of bytes that can be read (or skipped over) from this input stream without blocking by the next caller of a method for this input stream.
	 * Overridden method
	 */
	public int available() {
		try {
			if ((encodedIn.available() == 0 && (repeatedNeedles == 0 && readBytes <= 0)) || (error)) {
				availableData = 0;
			}
		} catch (IOException e) {
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
		availableData = 0;
		readBytes = 0;
		nextRepeater = 0;
		nextNeedleIdxOffset = 1;
		repeatedNeedles = 0;
	}
	
	/**
	 * Reads the next needle from the raw input stream.
	 * Implemented method 
	 */
	public int read() throws IOException {
		checkStreamsDataAvailability();
		try {
			if (readBytes == 0) {
				nextRepeater = 0;
				readBytes = encodedIn.read(data);
			}
			decode();
			if (nextNeedleIdxOffset == 0) { // meaning that needle should be read in next cycle; repeater has been read already  
				// checking encoded stream
				if (encodedIn.available() == 0) { 
					error = true; // there is no data in stream, it is translated as error
				}
			}
		} catch (IOException e) {
			error = true;
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return needle;
	}
	
	/**
	 * implements RLE decoding algorithm 
	 * @throws IOException 
	 */
	protected void decode() {
		if (repeatedNeedles == 0) {
			if (readBytes == -1) {
				// since input stream is encoded stream so we don't know real number of available data for decoded stream
				availableData = 0;
				if (nextNeedleIdxOffset == 0) {
					error = true;
				}
				return;
			}
			//... but we can assume that real value available data will be at least 1
			availableData = 1;
			repeatedNeedles = data[nextRepeater];
			// some number of repeated bytes has been read, but needle will be read in next cycle
			if (nextRepeater + 1 >= readBytes) {
				readBytes = 0; // tells that we have to read more bytes
				nextNeedleIdxOffset = 0; // ... and first data will be needle, not repeater
			}
			else {
				int needleIdx = nextRepeater + nextNeedleIdxOffset;
				needle = data[needleIdx];
				nextRepeater += 2;
				if (nextRepeater >= readBytes) { // no more data, preparing members for next cycle
					readBytes = 0;
					nextNeedleIdxOffset = 1;
				}
				repeatedNeedles--;
			}
		}
		else {
			if (nextNeedleIdxOffset == 0) { // from the previous cycle we have read repeater only and we now have to read the needle
				nextNeedleIdxOffset = 1; // ... next repeater and needle will be read as usual
				needle = data[0]; // ... reading needle
				nextRepeater++;
				if (nextRepeater >= readBytes) { // no more data, preparing members for next cycle
					readBytes = 0;
				}				
			}
			repeatedNeedles--;
		}
	}
}
