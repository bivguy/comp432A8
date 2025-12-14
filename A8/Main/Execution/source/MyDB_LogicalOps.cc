
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!
	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (131072, 4028, "tempFile");
	MyDB_TableReaderWriterPtr inputTableRW = make_shared<MyDB_TableReaderWriter>(this->inputSpec, myMgr);
	MyDB_TableReaderWriterPtr outputTableRW = make_shared<MyDB_TableReaderWriter>(this->outputSpec, myMgr);

	vector<string> projections;
	for (auto& att : outputSpec->getSchema()->getAtts())
		projections.push_back("[" + att.first + "]");
	
	string predicate = "";
	if (this->selectionPred.size() > 0) {
		predicate = this->selectionPred[0]->toString();
	}

	for (int i = 1; i < this->selectionPred.size(); i++)
		predicate = "&& (" + this->selectionPred[i]->toString() + ", " + predicate + ")";

	shared_ptr<RegularSelection> regSelection = make_shared<RegularSelection>(inputTableRW, outputTableRW, predicate, projections);

	regSelection->run();

	return outputTableRW;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!
	MyDB_TableReaderWriterPtr lTable = this->leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	MyDB_TableReaderWriterPtr rTable = this->rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(this->outputSpec, lTable->getBufferMgr());

	int lTableSize = lTable->getNumPages();
	int rTableSize = rTable->getNumPages();
	
	// TODO: see if this is the right way to get the buffer size
	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (131072, 4028, "tempFile");
	int bufferSize = myMgr->getNumPages();
	
	// create final selection predicate
	string finalSelectionPredicate = "";
	if (this->outputSelectionPredicate.size() > 0) {
		finalSelectionPredicate = this->outputSelectionPredicate[0]->toString();
		for (int i = 1; i < this->outputSelectionPredicate.size(); i++) {
			finalSelectionPredicate = "&& (" + finalSelectionPredicate + ", " + this->outputSelectionPredicate[i]->toString() + ")";
		}
	}

	// create projections
	vector<string> projections;
	for (auto& att: outputSpec->getSchema()->getAtts()) {
		projections.push_back("[" + att.first + "]");
	}

	// create equality checks
	vector <pair <string, string>> equalityChecks;
	for (auto& att: this->outputSelectionPredicate) {
		if (att->isEq()) {
			ExprTreePtr lhs = att->getLHS();
			ExprTreePtr rhs = att->getRHS();

			equalityChecks.push_back(make_pair(lhs->toString(), rhs->toString()));
		}
	}

	// create left and right selection predicates 
	string leftSelectionPredicate = "";
	string rightSelectionPredicate = "";
	for (auto& att: this->outputSelectionPredicate) {
		bool inLeft = att->referencesTable(this->leftInputOp->getOutputTable()->getName());
		bool inRight = att->referencesTable(this->rightInputOp->getOutputTable()->getName());

		if (inLeft && !inRight) {
			if (leftSelectionPredicate == "") {
				leftSelectionPredicate = att->toString();
			} else {
				leftSelectionPredicate = "&& (" + leftSelectionPredicate + ", " + att->toString() + ")";
			}
		} else if (!inLeft && inRight) {
			if (rightSelectionPredicate == "") {
				rightSelectionPredicate = att->toString();
			} else {
				rightSelectionPredicate = "&& (" + rightSelectionPredicate + ", " + att->toString() + ")";
			}
		}
	}

	// choose between scan join and sort merge join
	if (lTableSize * 2 < bufferSize || rTableSize * 2 < bufferSize) {
		// TODO: implement this
		shared_ptr<ScanJoin> scanJoin = make_shared<ScanJoin>(
			lTable,
			rTable,
			output,
			finalSelectionPredicate,
			projections,
			equalityChecks,
			leftSelectionPredicate,
			rightSelectionPredicate
		);

		scanJoin->run();
	} else { 
		pair <string, string> equalityCheck = make_pair("", "");
		if (equalityChecks.size() > 0) {
			equalityCheck = equalityChecks[0];
		}
		// TODO: implement this
		shared_ptr<SortMergeJoin> sortMergeJoin = make_shared<SortMergeJoin>(
			lTable,
			rTable,
			output,
			finalSelectionPredicate,
			projections,
			equalityCheck,
			leftSelectionPredicate,
			rightSelectionPredicate
		);

		sortMergeJoin->run();
	}

	return output;
}

#endif
