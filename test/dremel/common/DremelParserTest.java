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

import java.io.File;
import java.io.IOException;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import static org.junit.Assert.*;
import static dremel.common.Drec.*;
/**
 *
 * @author camuelg
 */
public class DremelParserTest {
     @Test
     public void basic() throws RecognitionException, IOException {
        //tests parsing all SQL that are encountered in the documentation
        for(int i = 1; i <= 15; i++) {

            File tempFile = Drec.getFile("q"+i+"_temp.bql.ast");
            File expectedFile = Drec.getFile("q"+i+".bql.ast");
            File queryFile = Drec.getFile("q"+i+".bql");

            FileUtils.writeStringToFile(tempFile,
                    DremelParser.parseBql(FileUtils.readFileToString(
                        queryFile)).toStringTree());

            assertTrue("ast files differs",
                    FileUtils.contentEquals(expectedFile,tempFile));

            FileUtils.forceDelete(tempFile);
         }
     }
     @Test
     public void analyzeAttempt() throws RecognitionException, IOException {
         File queryFile = Drec.getFile("q"+15+".bql");
         AstNode select_statement = DremelParser.parseBql(FileUtils.readFileToString(queryFile));
         assertNull(select_statement.parent);
         assertTrue(select_statement.getType() == BqlParser.N_SELECT_STATEMENT);
         assertTrue(select_statement.getChild(0).getType() == BqlParser.N_FROM);
         assertTrue(select_statement.getChild(1).getType() == BqlParser.N_SELECT);
         assertTrue(select_statement.getChild(2).getType() == BqlParser.N_WHERE);
         assertTrue(select_statement.getChild(0).getChild(0).getChild(0).getText().equals("t"));
         assertTrue(select_statement.getChild(0).getChild(0).getChild(0).getType() == BqlParser.ID);
         
         AstNode node = (AstNode) select_statement.getChild(0).getChild(0).getChild(0);
         int line = select_statement.getChild(0).getChild(0).getChild(0).getLine();
         int index = select_statement.getChild(0).getChild(0).getChild(0).getCharPositionInLine();
         
         assertTrue(line == 6);
         assertTrue(index == 1);
         
     }

}