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
package de.uni_mannheim.informatik.dws.searchjoin.cli;

import java.io.File;
import java.util.List;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.searchjoin.index.TableToDocumentConverter;
import de.uni_mannheim.informatik.dws.searchjoin.index.WebTableIndexEntry;
import de.uni_mannheim.informatik.dws.searchjoin.index.WebTableIndexManager;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.TableFactory;

/**
 * Searches the index for matching tables given a query table
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableSearch extends Executable {

	@Parameter(names = "-index", required=true)
	private String indexLocation;
	
	public static void main(String[] args) {
		TableSearch app = new TableSearch();
		
		if(app.parseCommandLine(TableSearch.class, args)) {
			app.run();
		}
	}
	
	public void run() {
		// create the index and index manager
		IIndex index = new DefaultIndex(indexLocation);
		WebTableIndexManager idx = new WebTableIndexManager(index, new TableToDocumentConverter());
		
		// for each query table passed from the command line
		for(String p : params) {
			
			// load the query table
			TableFactory fac = new TableFactory();
			
			Table t = fac.createTableFromFile(new File(p));
			
			// search matching tables
			List<WebTableIndexEntry> result = idx.search(t);
			
			// print the results
			System.out.println(String.format("Search results for table %s", t.getPath()));
			for(WebTableIndexEntry entry : result) {
				System.out.println(String.format("\t%s", entry.getTablePath()));
			}
			
		}
		
	}
}
