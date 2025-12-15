
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"

const bool DEBUG_JOIN_OP = false;
const bool DEBUG_SCAN = false;

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!
	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (131072, 4028, "tempFile");
	MyDB_TableReaderWriterPtr inputTableRW = make_shared<MyDB_TableReaderWriter>(this->inputSpec, myMgr);
	MyDB_TableReaderWriterPtr outputTableRW = make_shared<MyDB_TableReaderWriter>(this->outputSpec, myMgr);

	vector<string> projections;
	for (auto& att : outputSpec->getSchema()->getAtts()) {
		projections.push_back("[" + att.first + "]");
		if (DEBUG_SCAN) {
			cout << "Adding projection: " << "[" + att.first + "]" << "\n" << flush;
		}
	}
	
	string predicate = "== (string[F], string[F])";
	if (this->selectionPred.size() > 0) {
		predicate = this->selectionPred[0]->toString();
	}

	for (int i = 1; i < this->selectionPred.size(); i++)
		predicate = "&& (" + this->selectionPred[i]->toString() + ", " + predicate + ")";

	if (DEBUG_SCAN) {
		cout << "predicate is: " << predicate << "\n" << flush;
	}
	shared_ptr<RegularSelection> regSelection = make_shared<RegularSelection>(inputTableRW, outputTableRW, predicate, projections);

	regSelection->run();

	return outputTableRW;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// your code here!
	if (DEBUG_JOIN_OP) {
		cout << "About to get left table\n" << flush;
	}
	MyDB_TableReaderWriterPtr lTable = this->leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	if (DEBUG_JOIN_OP) {
		cout << "About to get right table\n" << flush;
	}
	MyDB_TableReaderWriterPtr rTable = this->rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
	MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(this->outputSpec, lTable->getBufferMgr());

	int lTableSize = lTable->getNumPages();
	int rTableSize = rTable->getNumPages();
	
	// TODO: see if this is the right way to get the buffer size
	MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (131072, 4028, "tempFile");
	int bufferSize = myMgr->getNumPages();
	
	string defaultPred = "== (string[F], string[F])";

	// create final selection predicate
	string finalSelectionPredicate = defaultPred;
	if (this->outputSelectionPredicate.size() > 0) {
		finalSelectionPredicate = this->outputSelectionPredicate[0]->toString();
		if (DEBUG_JOIN_OP) {
			cout << "At outputSelectionPredicate " << this->outputSelectionPredicate[0]->toString() << "\n" << flush;
		}
		for (int i = 1; i < this->outputSelectionPredicate.size(); i++) {
			finalSelectionPredicate = "&& (" + finalSelectionPredicate + ", " + this->outputSelectionPredicate[i]->toString() + ")";
			if (DEBUG_JOIN_OP) {
				cout << "At outputSelectionPredicate " << this->outputSelectionPredicate[i]->toString() << "\n" << flush;
			}
		}
	}

	if (DEBUG_JOIN_OP) {
		cout << "Final Selection Predicate: " << finalSelectionPredicate << "\n" << flush;
	}

	// create projections
	vector<string> projections;
	for (auto& att: outputSpec->getSchema()->getAtts()) {
		projections.push_back("[" + att.first + "]");

		if (DEBUG_JOIN_OP) {
			cout << "Adding projection: " << "[" + att.first + "]" << "\n" << flush;
		}
	}



	MyDB_SchemaPtr leftSchema = this->leftInputOp->getOutputTable()->getSchema();
    MyDB_SchemaPtr rightSchema = this->rightInputOp->getOutputTable()->getSchema();

	if (DEBUG_JOIN_OP) {
		for (auto& att: leftSchema->getAtts()) {
			cout << "Left Schema Attribute: " << att.first << "\n" << flush;
		}

		for (auto& att: rightSchema->getAtts()) {
			cout << "Right Schema Attribute: " << att.first << "\n" << flush;
		}
	}
	string leftTableName = this->leftInputOp->getOutputTable()->getName();
	string rightTableName = this->rightInputOp->getOutputTable()->getName();

	if (DEBUG_JOIN_OP) {
		cout << "Left Table Name: " << leftTableName << "\n" << flush;
		cout << "Right Table Name: " << rightTableName << "\n" << flush;
	}
	// create equality checks
	vector <pair <string, string>> equalityChecks;
	for (auto& att: this->outputSelectionPredicate) {
		if (att->isEq()) {
			ExprTreePtr lhs = att->getLHS();
			ExprTreePtr rhs = att->getRHS();
			pair<string, string> check = make_pair(lhs->toString(), rhs->toString());
			
			if (DEBUG_JOIN_OP) {
				cout << "At equality check: " << att->toString() << ")\n" << flush;

			}

			auto inSchema = [](string checkAtt, MyDB_SchemaPtr schema) {
                string dec = checkAtt;
                // Remove brackets if necessary
                if (dec.size() > 2 && dec.front() == '[' && dec.back() == ']') {
                    dec = dec.substr(1, dec.size() - 2);
                }
                for (auto& att : schema->getAtts()) {
                    if (att.first == dec) return true;
                }
                return false;
            };

			bool leftReferencesLeft = inSchema(lhs->toString(), leftSchema);
			bool rightReferencesRight = inSchema(rhs->toString(), rightSchema);

			bool leftReferencesRight = inSchema(lhs->toString(), rightSchema);
			bool rightReferencesLeft = inSchema(rhs->toString(), leftSchema);

			if (leftReferencesLeft && rightReferencesRight) {
				check = make_pair(lhs->toString(), rhs->toString());
			} else if (leftReferencesRight && rightReferencesLeft) {
				check = make_pair(rhs->toString(), lhs->toString());
			} else {
				continue;
			}

			equalityChecks.push_back(check);
			if (DEBUG_JOIN_OP) {
				cout << "Adding equality check: " << "(" << check.first << ", " << check.second << ")\n" << flush;
			}
		}
	}

	// create left and right selection predicates 
	string leftSelectionPredicate = defaultPred;
	string rightSelectionPredicate = defaultPred;
	for (auto& att: this->outputSelectionPredicate) {
		bool inLeft = att->referencesTable(leftTableName);
		bool inRight = att->referencesTable(rightTableName);

		if (inLeft && !inRight) {
			if (leftSelectionPredicate == defaultPred) {
				leftSelectionPredicate = att->toString();
			} else {
				leftSelectionPredicate = "&& (" + leftSelectionPredicate + ", " + att->toString() + ")";
			}
		} else if (!inLeft && inRight) {
			if (rightSelectionPredicate == defaultPred) {
				rightSelectionPredicate = att->toString();
			} else {
				rightSelectionPredicate = "&& (" + rightSelectionPredicate + ", " + att->toString() + ")";
			}
		}
	}

	if (DEBUG_JOIN_OP) {
		cout << "Left Selection Predicate: " << leftSelectionPredicate << "\n" << flush;
		cout << "Right Selection Predicate: " << rightSelectionPredicate << "\n" << flush;
	}

	// choose between scan join and sort merge join
	if (lTableSize * 2 < bufferSize || rTableSize * 2 < bufferSize) {
		if (DEBUG_JOIN_OP) {
			cout << "Using Scan Join\n" << flush;
		}

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

		if (DEBUG_JOIN_OP) {
			cout << "Using Sort Merge Join with equality check " << "(" << equalityCheck.first << ", " << equalityCheck.second << ")" << "\n" << flush;
		}
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

	lTable->getBufferMgr()->killTable(lTable->getTable());
	rTable->getBufferMgr()->killTable(rTable->getTable());
	return output;
}

#endif
