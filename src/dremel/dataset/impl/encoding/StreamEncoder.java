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

import java.io.OutputStream;


/**
 * Stream's encoder interface.  
 * @author babay
 *
 */
public abstract class StreamEncoder extends OutputStream {

	protected OutputStream encodedOut;
	/**
	 * Sets output stream where encoded data will be streamed 	
	 * @param in
	 */
	public void setOutputStream(OutputStream encodedOut) {
		this.encodedOut = encodedOut;
	}
	
	/**
	 * Closes this output stream and releases any system resources associated with this stream.
	 */
	public void close() {
		
	}
    
	/**
	 * Flushes this output stream and forces any buffered output bytes to be written out.
	 */
	public void flush() {
		
	}
    
	/**
	 * Writes b.length bytes from the specified byte array to this output stream. 
	 */
	public void write(byte[] b) {
		for(int i = 0; i < b.length; i++) {
			write(b[i]);
		}
	}
    
	/**
	 * Writes len bytes from the specified byte array starting at offset off to this output stream. 
	 */
	public void write(byte[] b, int off, int len) {
		int l = off + len;
		for(int i = off; i < l; i++) {
			write(b[i]);
		}
	}
    
	/**
	 * Writes the specified byte to this output stream.
	 */
	public abstract void write(int b);
}

