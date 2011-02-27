/**
 * Copyright 2010, Petascan Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.Ope
 */

package dremel.dataset.encoding;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;


import dremel.dataset.impl.encoding.rle.*;


/**
 * 
 * @author babay
 */
public class RleTest {
	
	static int maxOSArraySize = 256;
	
	@Test
	public void streamTest()
	{
		
		byte[] b1 = new byte[3];
		b1[0] = 0;
		b1[1] = 2;
		b1[2] = 2;
		/*b1[3] = 3;
		b1[4] = 3;
		b1[5] = 5;
		b1[6] = 5;
		b1[7] = 7;
		b1[8] = 7;
		b1[9] = 7;
		b1[10] = 9; */
		
		/*
		byte[] b1 = new byte[1];
		b1[0] = 0; */
		
		File  f = new File("testdata\\rleTest");
		
		DataOutputStream dataOutput = null;
		try {
			dataOutput = new DataOutputStream(new FileOutputStream(f));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 
		
		RleEncoderImpl rleEncoder= RleEncoderImpl.instance(dataOutput);
		
		DataOutputStream dataOut = new DataOutputStream(rleEncoder);
		
		for(int i=0; i<b1.length; i++)
		{
			//rleEncoder.write(b1[i]);
			try {
				dataOut.write(b1[i]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			dataOut.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//rleEncoder.close();
		
		DataInputStream dataInput = null;
		try {
			dataInput = new DataInputStream(new FileInputStream(f));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		RleDecoderImpl rleDecoder = RleDecoderImpl.instance(dataInput);
		
		DataInputStream decodedDataInput = new DataInputStream(rleDecoder);
		
		int index = 0;
		while(true)
		{
			try
			{
				//int nextValue = rleDecoder.read();
				int nextValue = decodedDataInput.readByte();
				assertTrue(nextValue == b1[index]);
				index++;
			}catch(Exception ex)
			{
				break;
			}
		}
		
	}
	
	
	@Test
	public void encoderDecoderTest1() throws IOException {
		byte[] b1 = new byte[11];
		b1[0] = 0;
		b1[1] = 1;
		b1[2] = 1;
		b1[3] = 3;
		b1[4] = 3;
		b1[5] = 5;
		b1[6] = 5;
		b1[7] = 7;
		b1[8] = 7;
		b1[9] = 7;
		b1[10] = 9;

		byte[] b2 = new byte[5];
		b2[0] = 0;
		b2[1] = 1;
		b2[2] = 2;
		b2[3] = 3;
		b2[4] = 4;
		
		byte[] b3 = new byte[3];
		b3[0] = 0;
		b3[1] = 1;
		b3[2] = 1;
		
		byte[] b4 = new byte[5];
		b4[0] = 2;
		b4[1] = 2;
		b4[2] = 2;
		b4[3] = 2;
		b4[4] = 2;

		byte[] b5 = new byte[1];
		b5[0] = 4;

		byte[][] bb = new byte[5][];
		
		bb[0] = b1;
		bb[1] = b2;
		bb[2] = b3;
		bb[3] = b4;
		bb[4] = b5;
		
		for(int l = 0; l < bb.length; l++) {
			java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
			RleEncoderImpl encoder = RleEncoderImpl.instance(bos);
			for(int i = 0; i < bb[l].length; i++) {
				encoder.write(bb[l][i]);
			}
			encoder.flush();
			java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bos.toByteArray());
			RleDecoderImpl decoder = RleDecoderImpl.instance(bis);
			for(int i = 0; decoder.available() != 0; i++) {
				byte t = (byte)decoder.read();
				if (!decoder.isError()) {
					assertTrue(bb[l][i] == t);
					//System.out.println("bb[l][i] '" + bb[l][i] + "'; t '" + t + "'");
				}
			}
			//System.out.println("---------------------------");
		}		
	}
	
}
