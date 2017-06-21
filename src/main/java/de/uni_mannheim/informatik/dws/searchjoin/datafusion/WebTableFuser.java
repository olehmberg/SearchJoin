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
package de.uni_mannheim.informatik.dws.searchjoin.datafusion;

import java.io.IOException;

import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableRowFactory;
import de.uni_mannheim.informatik.dws.winter.datafusion.AttributeFuser;
import de.uni_mannheim.informatik.dws.winter.datafusion.CorrespondenceSet;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionEngine;
import de.uni_mannheim.informatik.dws.winter.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.dws.winter.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.Voting;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * Fuses the query and result datasets using the provided record correspondences. 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableFuser {

	public Table fuseTables(
			Table consolidatedSchema, 
			FusibleDataSet<MatchableTableRow, MatchableTableColumn> queryDS,
			FusibleDataSet<MatchableTableRow, MatchableTableColumn> tablesDS, 
			Processable<Correspondence<MatchableTableRow, Matchable>> recordCorrespondences) throws IOException {
		
		
		DataFusionStrategy<MatchableTableRow, MatchableTableColumn> fusionStrategy = new DataFusionStrategy<>(new MatchableTableRowFactory());
		
		for(MatchableTableColumn c : queryDS.getSchema().get()) {
			AttributeFuser<MatchableTableRow, MatchableTableColumn> fuser = new WebTableValueFuser(new Voting<>(), c);
			
			EvaluationRule<MatchableTableRow, MatchableTableColumn> rule = new WebTableFusionEvaluationRule();
			
			fusionStrategy.addAttributeFuser(c, fuser, rule);
		}
		
		DataFusionEngine<MatchableTableRow, MatchableTableColumn> fusion = new DataFusionEngine<>(fusionStrategy);
	
		CorrespondenceSet<MatchableTableRow, MatchableTableColumn> correspondences = new CorrespondenceSet<>();
		correspondences.createFromCorrespondences(recordCorrespondences, queryDS, tablesDS);
		
		FusibleDataSet<MatchableTableRow, MatchableTableColumn> fused = fusion.run(correspondences, null);
		
		Table result = consolidatedSchema.copySchema();
		
		int rowNumber = 0;
		for(MatchableTableRow r : fused.get()) {
			TableRow row = new TableRow(rowNumber++, result);
			row.set(r.getValues());
			result.addRow(row);
		}
		
		return result;
	}
	
}
