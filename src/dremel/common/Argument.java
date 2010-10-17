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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author camuelg
 */
public class Argument {

    private static final Log LOG = LogFactory.getLog("openDremel.console.Argument");
    
    protected String name;
    protected String value;

    public Argument(String[] args, int position, String name) throws Exception {
        LOG.info("Looking for " + name + "....  ");
        if (args.length > position) {
            this.value = args[position];
            this.name = name;
            LOG.info("found " + value);
        } else {
            LOG.info("not found.");
            throw new Exception("Argument " + name + " not supplied");
        }
    };

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

};