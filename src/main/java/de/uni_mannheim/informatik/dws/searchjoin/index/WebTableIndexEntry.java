package de.uni_mannheim.informatik.dws.searchjoin.index;
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


import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;


/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableIndexEntry {

	private String values;
	private String tablePath;
	private String dbpediaClass;
	
	public static final String VALUES_FIELD = "values";
	public static final String TABLE_PATH_FIELD = "table_path";
//	public static final String DBPEDIA_CLASS_FIELD = "dbpedia_class";
	
	public static WebTableIndexEntry fromDocument(Document doc)
	{
		WebTableIndexEntry e = new WebTableIndexEntry();
		
//		e.setValues(doc.getField(VALUES_FIELD).stringValue());
		e.setTablePath(doc.getField(TABLE_PATH_FIELD).stringValue());
//		e.setDbpediaClass(doc.getField(DBPEDIA_CLASS_FIELD).stringValue());

		return e;
	}
	
	public Document createDocument()
	{
		Document doc = new Document();
		
		doc.add(new TextField(VALUES_FIELD, values, Field.Store.NO));
		doc.add(new StoredField(TABLE_PATH_FIELD, tablePath));
//		doc.add(new StringField(DBPEDIA_CLASS_FIELD, dbpediaClass, Field.Store.YES));
		
		return doc;
	}

	public String getValues() {
		return values;
	}

	public void setValues(String entityLabels) {
		this.values = entityLabels;
	}

//	public String getDbpediaClass() {
//		return dbpediaClass;
//	}
//
//	public void setDbpediaClass(String dbpediaClass) {
//		this.dbpediaClass = dbpediaClass;
//	}

	public String getTablePath() {
		return tablePath;
	}

	public void setTablePath(String tablePath) {
		this.tablePath = tablePath;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dbpediaClass == null) ? 0 : dbpediaClass.hashCode());
		result = prime * result + ((values == null) ? 0 : values.hashCode());
		result = prime * result + ((tablePath == null) ? 0 : tablePath.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WebTableIndexEntry other = (WebTableIndexEntry) obj;
		if (dbpediaClass == null) {
			if (other.dbpediaClass != null)
				return false;
		} else if (!dbpediaClass.equals(other.dbpediaClass))
			return false;
		if (values == null) {
			if (other.values != null)
				return false;
		} else if (!values.equals(other.values))
			return false;
		if (tablePath == null) {
			if (other.tablePath != null)
				return false;
		} else if (!tablePath.equals(other.tablePath))
			return false;
		return true;
	}


}
