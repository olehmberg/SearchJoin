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
package de.uni_mannheim.informatik.dws.searchjoin.matching;

import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.EnsembleMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.InstanceBasedRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.InstanceBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * Implements schema and record matching.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableMatcher {

	public Processable<Correspondence<MatchableTableColumn, Matchable>> matchSchemas(DataSet<MatchableTableRow, MatchableTableColumn> query, DataSet<MatchableTableRow, MatchableTableColumn> tables) throws Exception {

		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
	
		// match the query table to the found tables
		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> labelCors = engine.runLabelBasedSchemaMatching(query.getSchema(), tables.getSchema(), new WebTableColumnHeaderComparator(), 1.0);
		Processable<Correspondence<MatchableTableColumn, MatchableValue>> valueCors = engine.runInstanceBasedSchemaMatching(query, tables, new InstanceBasedSchemaBlocker<>(new WebTableColumnValueGenerator()), new VotingAggregator<>(false, query.size(), 0.25));
		
		// match the found tables to each other
		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> labelCorsCorpus = engine.runLabelBasedSchemaMatching(tables.getSchema(), new WebTableColumnHeaderComparator(), 1.0);
		Processable<Correspondence<MatchableTableColumn, MatchableValue>> valueCorsCorpus = engine.runInstanceBasedSchemaMatching(tables, new InstanceBasedSchemaBlocker<>(new WebTableColumnValueGenerator()), new VotingAggregator<>(false, query.size(), 0.5));
		
		// combine all correspondences
		EnsembleMatchingAlgorithm<MatchableTableColumn, Matchable> ensemble = new EnsembleMatchingAlgorithm<>(new VotingAggregator<>(false, 1, 0.0));
		ensemble.addBaseMatcherResult(Correspondence.toMatchable(labelCors), 1.0);
		ensemble.addBaseMatcherResult(Correspondence.toMatchable(valueCors), 1.0);
		ensemble.addBaseMatcherResult(Correspondence.toMatchable(labelCorsCorpus), 1.0);
		ensemble.addBaseMatcherResult(Correspondence.toMatchable(valueCorsCorpus), 1.0);
		
		ensemble.run();
		
		return ensemble.getResult();
	}
	
	public Processable<Correspondence<MatchableTableRow, Matchable>> matchRecords(DataSet<MatchableTableRow, MatchableTableColumn> query, DataSet<MatchableTableRow, MatchableTableColumn> tables) {
		
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();

		Processable<Correspondence<MatchableTableRow, MatchableValue>> valueCors = engine.runSimpleIdentityResolution(query, tables, new InstanceBasedRecordBlocker<>(new WebTableRowEntityLabelValueGenerator()), new VotingAggregator<>(false, 1, 1.0));
		
		return Correspondence.toMatchable(valueCors);
		
	}
	
}
