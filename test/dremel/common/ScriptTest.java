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

import dremel.common.JavaLangScript;
import org.junit.Test;
import static org.junit.Assert.*;
/**
 *
 * @author camuelg
 */
public class ScriptTest {

    @Test
    public void compileOnceRunMany() throws Exception {

        //create Script object, precompiles script....
        JavaLangScript script = new JavaLangScript(
        		JaninoTest.JaninoTestReturnObject.class,
                    new String[]{
                        "static dremel.common.JaninoTest.JaninoTestReturnObject",
                        "static dremel.common.JaninoTest.JaninoTestException"},
                    new String[]{"a"}, new Class[]{int.class},
                    new Class[]{JaninoTest.JaninoTestException.class},
                        "JaninoTestReturnObject o = new JaninoTestReturnObject();"
                        + "o.a=a;"
                        + "if(a!=a) throw new JaninoTestException(\"a\"); "
                        + "return o;"
                 );


        JaninoTest.JaninoTestReturnObject retObj;
        Object[] params = new Object[]{1};

        //run precompiled script
        for(int i = 0; i < 1000*1000; i++) {
            params[0] = i;
            retObj = (JaninoTest.JaninoTestReturnObject) script.evaluate(params);
            assertEquals(i, retObj.a);
        }
    }
}