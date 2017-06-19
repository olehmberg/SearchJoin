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
package de.uni_mannheim.informatik.dws.searchjoin.data;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableDataSetLoader {
	
	public FusibleDataSet<MatchableTableRow, MatchableTableColumn> createQueryDataSet(Table t) {
		
		FusibleDataSet<MatchableTableRow,MatchableTableColumn> query = new FusibleParallelHashedDataSet<>();
		
		MatchableTableColumn[] cols = new MatchableTableColumn[t.getColumns().size()];
		int colIdx = 0;
		
		for(TableColumn col : t.getSchema().getRecords()) {
			MatchableTableColumn c = new MatchableTableColumn(0, col);
			query.addAttribute(c);
			cols[colIdx++] = c;
		}
		
		for(TableRow row : t.getRows()) {
			MatchableTableRow r = new MatchableTableRow(row, t.getTableId(), cols);
			query.add(r);
		}
		
		return query;
	}
	
	public FusibleDataSet<MatchableTableRow, MatchableTableColumn> createTablesDataSet(Collection<Table> tables) {
		FusibleDataSet<MatchableTableRow,MatchableTableColumn> result = new FusibleParallelHashedDataSet<>();
		
		int tableId=1;
		
		for(Table t : tables) {
			MatchableTableColumn[] cols = new MatchableTableColumn[t.getColumns().size()];
			int colIdx = 0;
			for(TableColumn col : t.getSchema().getRecords()) {
				MatchableTableColumn c = new MatchableTableColumn(tableId, col);
				result.addAttribute(c);
				cols[colIdx++] = c;
			}
			
			for(TableRow row : t.getRows()) {
				MatchableTableRow r = new MatchableTableRow(row, tableId, cols);
				result.add(r);
			}
			tableId++;
		}
		
		return result;
	}

}
