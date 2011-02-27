package dremel.dataset.impl.encoding;

import java.io.IOException;
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
	abstract public int read() throws IOException;
    
	/**
	 * Reads some number of bytes from the input stream and stores them into the buffer array b.
	 */
	public int read(byte[] b) throws IOException {
		int i = 0;
		for(; i < b.length && available() != 0; i++) {
			b[i] = (byte)read();
			if (isError()) {
				break;
			}			
		}
		if(i==0)
		{
			return -1;
		}
		return i;
	}
    
	/**
	 * Reads up to len bytes of data from the input stream into an array of bytes.
	 */
	public int read(byte[] b, int off, int len) throws IOException {
		int i = off;
		int l = off + len;
		for(; i < l && available() != 0; i++) {
			b[i] = (byte)read();
			if (isError()) {
				break;
			}
		}
		if(i==off)
		{
			return -1;
		}
		return i - off;
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
	
	/**
	 * Checks stream's availability of data and if there are no any available data then IOException is thrown
	 */
	public void checkStreamsDataAvailability() throws IOException {
		if (available() == 0) {
			error = true;
			if (encodedIn instanceof java.io.DataInputStream) {
				throw new java.io.EOFException(); 
			}
			else {
				throw new IOException();
			}
		}		
	}
}
