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
import de.uni_mannheim.informatik.dws.winter.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.ConflictResolutionFunction;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.FusedValue;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.RecordGroup;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * A value fuser for {@link MatchableTableRow}s for a specific {@link MatchableTableColumn}.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableValueFuser extends AttributeValueFuser<Object, MatchableTableRow, MatchableTableColumn> {

	private MatchableTableColumn targetColumn;
	
	/**
	 * 
	 * @param conflictResolution
	 * 			The conflict resolution function that this fuser should apply
	 * @param targetColumn
	 * 			The {@link MatchableTableColumn} that is fused by this fuser
	 */
	public WebTableValueFuser(
			ConflictResolutionFunction<Object, MatchableTableRow, MatchableTableColumn> conflictResolution, MatchableTableColumn targetColumn) {
		super(conflictResolution);
		this.targetColumn = targetColumn;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.datafusion.AttributeValueFuser#getValue(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	protected Object getValue(MatchableTableRow record,
			Correspondence<MatchableTableColumn, Matchable> correspondence) {
		
		if(record.hasValue(targetColumn)) {
			Object value = record.get(targetColumn.getColumnIndex());
			
			return value;
		} else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.datafusion.AttributeFuser#fuse(de.uni_mannheim.informatik.dws.winter.model.RecordGroup, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable, de.uni_mannheim.informatik.dws.winter.model.Matchable)
	 */
	@Override
	public void fuse(RecordGroup<MatchableTableRow, MatchableTableColumn> group, MatchableTableRow fusedRecord,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences,
			MatchableTableColumn schemaElement) {
		FusedValue<Object, MatchableTableRow, MatchableTableColumn> fused = getFusedValue(group, schemaCorrespondences, schemaElement);
		
		if(fused.getValue()!=null) {
			fusedRecord.set(targetColumn.getColumnIndex(), fused.getValue());
		}
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.datafusion.AttributeFuser#hasValue(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Correspondence)
	 */
	@Override
	public boolean hasValue(MatchableTableRow record,
			Correspondence<MatchableTableColumn, Matchable> correspondence) {
		return record.hasValue(targetColumn);
	}

}
