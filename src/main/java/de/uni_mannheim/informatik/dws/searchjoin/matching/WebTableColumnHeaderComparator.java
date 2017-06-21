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
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * Compares the headers of {@link MatchableTableColumn}s using generalised jaccard with inner levenshtein similarity
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableColumnHeaderComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	private GeneralisedStringJaccard sim = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.8, 0.0);
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		if("NULL".equals(record1.getHeader()) || "NULL".equals(record2.getHeader())) {
			return 0.0;
		} else {
			return sim.calculate(record1.getHeader(), record2.getHeader());
		}
	}

}
