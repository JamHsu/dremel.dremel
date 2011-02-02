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

/**
 *
 * This is parser-related class. 
 * This class represent a node in abstract syntax tree in dremel project
 * 
 * See ANTLR reference/books for more information.
 *
 * @author camuelg
 */

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;

public final class AstNode extends CommonTree {
	public AstNode(Token payload) {
		super(payload);
	}

	@Override
	public String toString() {
		return super.toString();
	}
}
