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


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.management.IndexManagerBase;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * Manages the search for tables.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableIndexManager extends IndexManagerBase {

	private static final long serialVersionUID = 1L;
	private boolean verbose = false;
    public boolean isVerbose() {
        return verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
    
    private TableToDocumentConverter converter;
    
	public WebTableIndexManager(IIndex index, TableToDocumentConverter converter) {
		super(index, WebTableIndexEntry.VALUES_FIELD);
		this.converter = converter;
	}

	public List<WebTableIndexEntry> search(Table t) {
	    long start, setup=0, search=0, result=0;
	    start = System.currentTimeMillis();
		List<WebTableIndexEntry> results = new LinkedList<WebTableIndexEntry>();

		IndexSearcher indexSearcher = getIndex().getIndexSearcher();

		QueryParser queryParser = getQueryParserFromCache();
		
		try {
			String value = converter.convertTableToDocument(t);
			value = QueryParserBase.escape(value); 
			Query q = null;
			
			if(!isSearchExactMatches()) {
    			
			    value = WebTablesStringNormalizer.normaliseValue(value, true);

			    List<String> tokens = WebTablesStringNormalizer.tokenise(value, true);
			    
			    StringBuilder sb = new StringBuilder();
			    
			    for(String token : tokens) {
			        sb.append(token);
			        
			        if(getMaxEditDistance()>0) {
			            sb.append("~");
			            sb.append(getMaxEditDistance());
			        }
			        sb.append(" ");
			    }
			    
			    value = sb.toString();
    			
    			if(value.trim().length()>0) {
    			    q = queryParser.parse(value);
    			}
			} else {
			    if(value.trim().length()>0) {
			        q = new TermQuery(new Term(getDefaultField(), value));
			    }
			}
			
			if(q!=null) {
    			if(getFilterValues()!=null && getFilterValues().size()>0) {
    			    BooleanQuery filter = new BooleanQuery();
    			    
    			    for(String s : getFilterValues()) {
    			        filter.add(new TermQuery(new Term(getFilterField(), s)), Occur.SHOULD);
    			    }
    			    
    			    BooleanQuery all = new BooleanQuery();
    			    all.add(q, Occur.MUST);
    			    all.add(filter, Occur.MUST);
    			    
    			    q = all;
    			}
    			
    			if(isVerbose()) {
    			    System.out.println("Query: \n" + value + "\n" + q.toString());
    			}
    			
    			setup = System.currentTimeMillis() - start;
    			start = System.currentTimeMillis();
    			
    			int numResults = getNumRetrievedDocsFromIndex();
    			ScoreDoc[] hits = indexSearcher.search(q, numResults).scoreDocs;
    			
    			search = System.currentTimeMillis() - start;
    			start = System.currentTimeMillis();
    			
    			if(hits != null)
    			{
    			    if(isVerbose()) {
    			        System.out.println(" found " + hits.length + " hits");
    			    }
    				for (int i = 0; i < hits.length; i++) {
    					
    					Document doc = indexSearcher.doc(hits[i].doc);
    	
    					WebTableIndexEntry e = WebTableIndexEntry.fromDocument(doc);

    					if(isVerbose()) {
    						System.out.println(e.getTablePath() + ": " + e.getValues());
    					}
    					
    					results.add(e);
    				}
    			}
			} else {
			    if(isVerbose()) {
			        System.out.println(String.format("Empty query for '%s'", t.getPath()));
			    }
			}
			
			result = System.currentTimeMillis() - start;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e1) {
		    System.err.println(String.format("Parse exception for '%s'", t.getPath()));
			e1.printStackTrace();
		}
		if(isVerbose()) {
		    System.out.println(" returning " + results.size() + " documents");
		    System.out.println(String.format("setup: %d\tsearch: %d\tload: %d", setup, search, result));
		}
		return results;
	}

}
