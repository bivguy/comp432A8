
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

	return nullptr;
}

#endif
