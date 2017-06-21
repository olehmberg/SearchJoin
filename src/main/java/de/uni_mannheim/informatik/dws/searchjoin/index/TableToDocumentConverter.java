/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.searchjoin.index;

import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * Converts a {@link Table} into a document that can be indexed
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableToDocumentConverter {

	public String convertTableToDocument(Table t) {
		StringBuilder sb = new StringBuilder();
		for (TableRow row : t.getRows()) {
			Object keyValue = row.getKeyValue();
			if (keyValue != null) {
				String normalised = WebTablesStringNormalizer.normalise(keyValue.toString(), false);

				sb.append(normalised);
				sb.append(" ");
			}
		}
		return sb.toString();
	}
	
}
