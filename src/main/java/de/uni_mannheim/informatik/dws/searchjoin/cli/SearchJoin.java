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
package de.uni_mannheim.informatik.dws.searchjoin.cli;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.searchjoin.data.WebTableDataSetLoader;
import de.uni_mannheim.informatik.dws.searchjoin.datafusion.SearchJoinSchemaConsolidator;
import de.uni_mannheim.informatik.dws.searchjoin.datafusion.WebTableFuser;
import de.uni_mannheim.informatik.dws.searchjoin.index.TableToDocumentConverter;
import de.uni_mannheim.informatik.dws.searchjoin.index.WebTableIndexEntry;
import de.uni_mannheim.informatik.dws.searchjoin.index.WebTableIndexManager;
import de.uni_mannheim.informatik.dws.searchjoin.matching.WebTableMatcher;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.TopKCorrespondencesAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.AggregateBySecondRecordRule;
import de.uni_mannheim.informatik.dws.winter.matching.rules.FlattenAggregatedCorrespondencesRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.TableFactory;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;

/**
 * 
 * Runs a Search Join for the given query tables.
 * 
 * For each query table, the index is searched for matching tables.
 * Then, the schema of these tables and the query tables is matched and consolidated, which adds new attributes to the query table.
 * Next, the records of all tables are matched for find duplicates and the values of these duplicates are fused by voting.
 * Records that do not match the query table are ignored.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SearchJoin extends Executable {

	@Parameter(names = "-index", required=true)
	private String indexLocation;
	
	@Parameter(names = "-out", required=true)
	private String outputLocation;
	
	public static void main(String[] args) throws Exception {
		SearchJoin app = new SearchJoin();
		
		if(app.parseCommandLine(SearchJoin.class, args)) {
			app.run();
		}
	}
	
	public void run() throws Exception {
		// create the index and index manager
		IIndex index = new DefaultIndex(indexLocation);
		WebTableIndexManager idx = new WebTableIndexManager(index, new TableToDocumentConverter());
		
		Map<Integer, Table> tablesById = new HashMap<>();
		
		// iterate over all query tables passed from the command line
		for(String p : params) {
			
			/*******************************************************
			 * SEARCH
			 *******************************************************/
			
			TableFactory fac = new TableFactory();
			
			// load the query table
			Table t = fac.createTableFromFile(new File(p));
			tablesById.put(t.getTableId(), t);
			
			// search 
			List<WebTableIndexEntry> result = idx.search(t);
			
			if(result!=null && result.size()>0) {
			
				// load the tables from the search result
				Collection<Table> tables = new LinkedList<>();
				int tableId = 1;
				for(WebTableIndexEntry entry : result) {
					Table table = fac.createTableFromFile(new File(entry.getTablePath()));
					if(table!=null && !table.getPath().equals(t.getPath())) {
						table.setTableId(tableId++);
						tablesById.put(table.getTableId(), table);
						tables.add(table);
					}
				}

				// add the query table to the search results
				// it will match the query table perfectly and make sure that all records are kept in the final result
				// even if no other table contained a certain record
				Table query = fac.createTableFromFile(new File(p));
				query.setPath("query");
				query.setTableId(tableId);
				tablesById.put(query.getTableId(), query);
				tables.add(query);
				
				/*******************************************************
				 * SCHEMA MATCHING
				 *******************************************************/
				
				// load the tables into datasets
				WebTableDataSetLoader loader = new WebTableDataSetLoader();
				FusibleDataSet<MatchableTableRow, MatchableTableColumn> queryDS = loader.createQueryDataSet(t);
				FusibleDataSet<MatchableTableRow, MatchableTableColumn> tablesDS = loader.createTablesDataSet(tables);
				
				// run schema matching
				WebTableMatcher matcher = new WebTableMatcher();
				Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = matcher.matchSchemas(queryDS, tablesDS);
				
				// get the ids of all tables in the search result that could be matched
				Set<Integer> matchedTables = new HashSet<>(schemaCorrespondences.map(
						(Correspondence<MatchableTableColumn, Matchable> cor, DataIterator<Integer> c)
							-> c.next(new Integer(cor.getSecondRecord().getTableId()))).distinct().get()
						);
				
				// remove unmatched tables
				Processable<MatchableTableColumn> attributes = tablesDS.getSchema().where(((c)->matchedTables.contains(c.getTableId())));
	
				/*******************************************************
				 * SCHEMA CONSOLIDATION
				 *******************************************************/
				
				// transform query table and result tables into the consolidated schema
				attributes = queryDS.getSchema().append(attributes);
				SearchJoinSchemaConsolidator consolidator = new SearchJoinSchemaConsolidator(tablesById);
				Pair<Table, Table> consolidated = consolidator.consolidate(queryDS, tablesDS, attributes, schemaCorrespondences);
	
				if(consolidated!=null) {
				
					Table queryConsolidated = consolidated.getFirst();
					Table tablesConsolidated = consolidated.getSecond();
					
					// set the subject column to the column that was the subject column in the query table
					for(TableColumn c: queryConsolidated.getSchema().getRecords()) {
						if(c.getProvenance().contains(t.getSubjectColumn().getIdentifier())) {
							queryConsolidated.setSubjectColumnIndex(c.getColumnIndex());
							break;
						}
					}
					for(TableColumn c: tablesConsolidated.getSchema().getRecords()) {
						if(c.getProvenance().contains(t.getSubjectColumn().getIdentifier())) {
							tablesConsolidated.setSubjectColumnIndex(c.getColumnIndex());
							break;
						}
					}
	
					/*******************************************************
					 * IDENTITY RESOLUTION
					 *******************************************************/
					
					// create datasets from the consolidated tables
					queryDS = loader.createQueryDataSet(queryConsolidated);
					tablesDS = loader.createQueryDataSet(tablesConsolidated);
					
					// run identity resolution
					Processable<Correspondence<MatchableTableRow, Matchable>> recordCorrespondences = matcher.matchRecords(queryDS, tablesDS);
	
					// make sure that no two records from the query table are mapped to the same record in a result table
					// the result would be that these records are merged in the final result
					recordCorrespondences = recordCorrespondences
							.aggregate(
									new AggregateBySecondRecordRule<MatchableTableRow, Matchable>(0.0), 
									new TopKCorrespondencesAggregator<>(1))
							.map(new FlattenAggregatedCorrespondencesRule<>());
					
					/*******************************************************
					 * DATA FUSION
					 *******************************************************/
					
					// fuse the records into a final table
					WebTableFuser fuser = new WebTableFuser();
					
					Table fused = fuser.fuseTables(queryConsolidated, queryDS, tablesDS, recordCorrespondences);
					
					// remove columns that are mostly NULL
					fused = consolidator.removeSparseColumns(fused, 0.1);
					
					// write the final result
					CSVTableWriter w = new CSVTableWriter();
					File outF = new File(outputLocation);
					outF.mkdirs();
					w.write(fused, new File(outF, t.getPath() + "_result"));
				}
				
			} else {
				System.err.println(String.format("No results found for %s", t.getPath()));
			}
		
		}
	}
	
}
