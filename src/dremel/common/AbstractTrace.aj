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

package dremel.common;

import java.io.PrintStream;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;

/**
 * This class provides support for printing trace messages into a stream. The
 * trace messages consist of the class name, method name (if method) and the
 * list of parameter types.
 * <P>
 * The class is thread-safe. Different threads may use different output streams
 * by simply calling the method initStream(myStream).
 * <P>
 * This class should be extended. It defines 3 abstract crosscuts for injecting
 * the tracing functionality into any constructors and methods of any
 * application classes.
 * <P>
 * 
 * 
 * @author camuelg
 *
 */
public abstract aspect AbstractTrace {

	abstract pointcut classes();
	abstract pointcut constructors();
	abstract pointcut methods();

	before(): classes() && (methods() || constructors()) && !execution(* Object.*(..)) {
		doTraceEntry(thisJoinPoint);
	}

	after() returning(Object ret): classes() && (methods() || constructors()) && !execution(* Object.*(..)) {
		doTraceExit(thisJoinPoint, ret);
	}

	public void initStream(PrintStream _stream) {
		setStream(_stream);
	}

	private ThreadLocal stream = new ThreadLocal() {
		protected Object initialValue() {
			return System.err;
		}
	};
	private ThreadLocal callDepth = new ThreadLocal() {
		protected Object initialValue() {
			return new Integer(0);
		}
	};

	private PrintStream getStream() {
		return (PrintStream) stream.get();
	}

	private void setStream(PrintStream s) {
		stream.set(s);
	}

	private int getCallDepth() {
		return ((Integer) (callDepth.get())).intValue();
	}

	private void setCallDepth(int n) {
		callDepth.set(new Integer(n));
	}

	private void doTraceEntry(JoinPoint jp) {
		setCallDepth(getCallDepth() + 1);
		printEntering(jp);
	}

	private void doTraceExit(JoinPoint jp, Object ret) {
		printExiting(ret);
		setCallDepth(getCallDepth() - 1);
	}

	private void printEntering(JoinPoint jp) {
		printIndent();
		getStream().print("> ");
		getStream().print(jp);
		printParameters(jp);
	}

	private void printExiting(Object ret) {
		getStream().print("< ");
		printIndent();
		getStream().println("[Ret: "+ret+" ]");
		getStream().println();
	}

	private void printIndent() {
		for (int i = 0; i < getCallDepth(); i++)
			getStream().print("= ");
	}

	private String createParameterMessage(JoinPoint joinPoint) {
		StringBuffer paramBuffer = new StringBuffer("\n\t[This: ");
		paramBuffer.append(joinPoint.getThis());
		Object[] arguments = joinPoint.getArgs();
		paramBuffer.append("]\n\t[Args: (");
		for (int length = arguments.length, i = 0; i < length; ++i) {
			String argument = "" + arguments[i];
			argument = argument.length() > 100 ? argument.substring(0, 94)+"...   " : argument;
			paramBuffer.append(argument);
			if (i != length - 1) {
				paramBuffer.append(',');
			}
		}
		paramBuffer.append(")]");
		return paramBuffer.toString();
	}

	private void printParameters(JoinPoint jp) {
		Signature sig = jp.getSignature();
		getStream().println(
				"Entering [" + sig.getDeclaringType().getName() + "."
						+ sig.getName() + "]" + createParameterMessage(jp));
	}

}
