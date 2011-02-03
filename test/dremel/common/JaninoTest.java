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

import java.lang.reflect.InvocationTargetException;
import org.codehaus.commons.compiler.CompileException;
import org.junit.Test;
import static org.junit.Assert.*;

import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.commons.compiler.CompilerFactoryFactory;

/**
 *
 * @author camuelg
 */
public class JaninoTest {
	static public class JaninoTestReturnObject {
	    public int a;
	}
	static public class JaninoTestException extends Exception {
	    public JaninoTestException(String a) {super(a);};
	}

    void scriptCompileAndRun(int a, int b) throws InvocationTargetException, CompileException, Exception {
        IScriptEvaluator se = compile(b);

        JaninoTestReturnObject o =
                (JaninoTestReturnObject) se.evaluate(new Object[]{a, b});

        assertEquals(a + b, o.a);
    }

    // TODO - to implement arbitrary expressions with arbitrary return types. For now it 
    // is hardcord + operator with int result (JaninoTestReturnObject)
    protected IScriptEvaluator compile(int b)
            throws CompileException, Exception {
        IScriptEvaluator se =
                CompilerFactoryFactory.getDefaultCompilerFactory().
                newScriptEvaluator();
        se.setReturnType(JaninoTestReturnObject.class);
        se.setDefaultImports(new String[]{
                    "static dremel.common.JaninoTest.JaninoTestReturnObject",
                    "static dremel.common.JaninoTest.JaninoTestException"});
        se.setParameters(new String[]{"a", "b"},
                new Class[]{int.class, int.class});
        se.setThrownExceptions(new Class[]{JaninoTestException.class});
        se.cook("" + "JaninoTestReturnObject o = new JaninoTestReturnObject();" +
                " o.a=a+b; " + "if(a==" + b +
                ") /* so each code snippet will be unique and " +
                "                 it will not optimize by memoizing*/" +
                "     throw new JaninoTestException(\"a==b\"); " + "return o;");
        return se;
    }

    @Test
    public void scriptRunOnce() throws Exception {
        scriptCompileAndRun(999, 888);
    }

    @Test
    public void scriptRepitativeCompileAndRunPerformanceCheck() throws Exception {
        //warming up the system....
        for (int i = 0; i < 10; i++) {
            scriptCompileAndRun(i, i + 1);
        }
        //now in steady state, make time measurements hoping the machine is
        //not overloaded, must be under one second.
        final long startTimeNano = System.nanoTime();
        for (int i = 0; i < 200; i++) {
            scriptCompileAndRun(i, i + 1);
        }
        final long durationNano = System.nanoTime() - startTimeNano;
        assertTrue(durationNano < 1000 * 1000 * 1000);
    }

    @Test
    public void scriptRepitativeJustRunPerformanceCheck() throws Exception {
        IScriptEvaluator se = compile(-1);
        Object[] params = new Object[]{1, 2};
        JaninoTestReturnObject o;

        //warming up the system....
        for (int i = 0; i < 10; i++) {
            params[0] = i;
            params[1] = i+1;
            o = (JaninoTestReturnObject) se.evaluate(params);
            assertEquals(2 * i + 1, o.a);
        }
        //now in steady state, make time measurements hoping the machine is
        //not overloaded, must be under one second.
        final long startTimeNano = System.nanoTime();
        for (int i = 0; i < 1000*1000; i++) {
            params[0] = i;
            params[1] = i+1;
            o = (JaninoTestReturnObject) se.evaluate(params);
            assertEquals(2 * i + 1, o.a);
        }
        final long durationNano = System.nanoTime() - startTimeNano;
        assertTrue(durationNano < 1000 * 1000 * 1000);
    }

    @Test
    public void scriptRunOnceExceptionCase() throws CompileException, Exception {
        //TODO write exception handling case, it thows
        // InvocationTargetException, the real one is nested?
        boolean thrown = false;
        try {
            scriptCompileAndRun(5, 5);
        } catch (InvocationTargetException ex) {
            thrown = true;
        }

        assertTrue(thrown);
    }
}
