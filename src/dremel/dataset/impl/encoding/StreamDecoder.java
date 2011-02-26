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
package dremel.dataset.impl.encoding;

import java.io.InputStream;

/**
 * Stream's decode interface.  
 * @author babay
 *
 */
public abstract class StreamDecoder extends InputStream {

	protected InputStream encodedIn;
	protected boolean error = false;
	/**
	 * Sets output stream where decoded data will be streamed 	
	 * @param in
	 */
	public void setInStream(InputStream encodedIn) {
		this.encodedIn = encodedIn;
	}
	
	/**
	 * Returns the number of bytes that can be read (or skipped over) from this input stream without blocking by the next caller of a method for this input stream.
	 * Overridden method
	 */
	public int 	available() {
		return 0;
	}
    
	/**
	 * Closes this input stream and releases any system resources associated with the stream.
	 * Overridden method
	 */
	public void close() {
		
	}
    
	/**
	 *  Marks the current position in this input stream.
	 * Overridden method
	 */
	public void mark(int readlimit) {
		
	}
    
	/**
	 * Overriden method
	 * Tests if this input stream supports the mark and reset methods.
	 */
	public boolean markSupported() {
		return false;
	}
    
	/**
	 * Reads the next tuple of data from the raw input stream.
	 * Implemented method 
	 */
	abstract public int read();
    
	/**
	 * Reads some number of bytes from the input stream and stores them into the buffer array b.
	 */
	public int read(byte[] b) {
		for(int i = 0; i < b.length; i++) {
			b[i] = (byte)read();
			if (isError()) {
				return i;
			}			
		}
		return b.length;
	}
    
	/**
	 * Reads up to len bytes of data from the input stream into an array of bytes.
	 */
	public int read(byte[] b, int off, int len) {
		int l = off + len;
		for(int i = off; i < l; i++) {
			b[i] = (byte)read();
			if (isError()) {
				return (i - off);
			}
		}
		return len;
	}
    
	/**
	 * Repositions this stream to the position at the time the mark method was last called on this input stream.
	 * Overridden method 
	 */
	public void reset() {
		
	}
    
	/**
	 * Skips over and discards n bytes of data from this input stream.
	 * Overridden method
	 */
	public long skip(long n) {
		return 0;
	}
	
	/**
	 * Returns true if error was detected during read operation
	 * @return
	 */
	public boolean isError() {
		return error;
	}
}
