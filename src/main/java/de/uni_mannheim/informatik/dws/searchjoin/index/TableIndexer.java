/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.searchjoin.index;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

/**
 * 
 * Adds tables to an index
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableIndexer {

	private IIndex index;
	private IndexWriter writer;
	private TableToDocumentConverter converter;

	public TableIndexer(IIndex index, TableToDocumentConverter converter) {
		this.index = index;
		writer = index.getIndexWriter();
		this.converter = converter;
	}

	public void indexTable(Table t, File f) {
		if (!t.hasSubjectColumn()) {
			t.identifySubjectColumn();
		}

		if (t.hasSubjectColumn()) {
			WebTableIndexEntry e = new WebTableIndexEntry();
			e.setValues(converter.convertTableToDocument(t));
			e.setTablePath(f.getAbsolutePath());

			try {
				writer.addDocument(e.createDocument());
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void closeWriter() {
		index.closeIndexWriter();
	}

}
