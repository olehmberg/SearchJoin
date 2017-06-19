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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.winter.model.FusableFactory;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroup;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowFactory implements FusableFactory<MatchableTableRow, MatchableTableColumn> {

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.model.FusableFactory#createInstanceForFusion(de.uni_mannheim.informatik.dws.winter.model.RecordGroup)
	 */
	@Override
	public MatchableTableRow createInstanceForFusion(RecordGroup<MatchableTableRow, MatchableTableColumn> cluster) {
		
		List<String> ids = new LinkedList<>();

		for (MatchableTableRow m : cluster.getRecords()) {
			ids.add(m.getIdentifier());
		}

		Collections.sort(ids);

		String mergedId = StringUtils.join(ids, '+');
		
		return new MatchableTableRow(mergedId, Q.firstOrDefault(cluster.getRecords()).getSchema());
	}

}
