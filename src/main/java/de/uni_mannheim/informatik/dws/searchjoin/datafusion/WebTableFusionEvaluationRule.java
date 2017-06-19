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

import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.searchjoin.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableFusionEvaluationRule extends EvaluationRule<MatchableTableRow, MatchableTableColumn> {

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.datafusion.EvaluationRule#isEqual(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable)
	 */
	@Override
	public boolean isEqual(MatchableTableRow record1, MatchableTableRow record2, MatchableTableColumn schemaElement) {
		Object value1 = record1.get(schemaElement.getColumnIndex());
		Object value2 = record2.get(schemaElement.getColumnIndex());
		
		return Q.equals(value1, value2, true);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.datafusion.EvaluationRule#isEqual(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public boolean isEqual(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
		Object value1 = record1.get(schemaCorrespondence.getFirstRecord().getColumnIndex());
		Object value2 = record2.get(schemaCorrespondence.getSecondRecord().getColumnIndex());
		
		return Q.equals(value1, value2, true);
	}

}
