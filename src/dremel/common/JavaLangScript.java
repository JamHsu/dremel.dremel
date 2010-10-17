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
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

/**
 *
 * @author camuelg
 */
public class JavaLangScript {
    protected IScriptEvaluator se;
    public JavaLangScript(          Class returnType,
                            String[] imports,
                            String[] paramNames,
                            Class[]  paramTypes,
                            Class[]  exceptions,
                            String   script
                  ) throws Exception  {

        se = CompilerFactoryFactory.getDefaultCompilerFactory().
                newScriptEvaluator();
        se.setReturnType(returnType);
        se.setDefaultImports(imports);
        se.setParameters(paramNames, paramTypes);
        se.setThrownExceptions(exceptions);
        se.cook(script);

    };
    public Object evaluate(Object[] params) throws InvocationTargetException {
        return se.evaluate(params);
    };
}
