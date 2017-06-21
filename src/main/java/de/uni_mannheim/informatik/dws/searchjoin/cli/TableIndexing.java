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

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.searchjoin.index.TableIndexer;
import de.uni_mannheim.informatik.dws.searchjoin.index.TableToDocumentConverter;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.parallel.ParallelProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.TableFactory;

/**
 * 
 * Reads all tables that are passed as commands (either as file names or directory names) and adds them to the index.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableIndexing extends Executable {

	@Parameter(names = "-index", required=true)
	private String indexDir;
	
	public static void main(String[] args) {
		TableIndexing ti = new TableIndexing();
		
		if(ti.parseCommandLine(TableIndexing.class, args)) {
			ti.run();
		}
	}
	
	public void run() {
		
		// create the index and indexer
		IIndex index = new DefaultIndex(indexDir);
		TableIndexer ti = new TableIndexer(index, new TableToDocumentConverter());
		
		// get all files that were passed from the command line
		Processable<File> filesToIndex = new ParallelProcessableCollection<>();
		
		for(String p : params) {
			File f = new File(p);
			
			if(f.isDirectory()) {
				for(File f1 : f.listFiles()) {
					filesToIndex.add(f1);
				}
			} else {
				filesToIndex.add(f);
			}
		}
		
		System.out.println(String.format("Indexing %d tables", filesToIndex.size()));
		
		// iterate over all files and add them to the index
		filesToIndex.iterate(
				(f) -> {
					
					TableFactory fac = new TableFactory();
					
					Table t = fac.createTableFromFile(f);
					
					if(t!=null) {
						ti.indexTable(t, f);
					}
					
				});
		
		// close the index writer
		ti.closeWriter();
		
		System.out.println("Done.");
	}
	
}
