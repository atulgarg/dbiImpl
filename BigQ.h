#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <queue>
#include <cmath>
#include <cstdlib>
#include <algorithm>
using namespace std;

class BigQ {
public:
	pthread_t worker;
	OrderMaker sortOrder;
	Pipe &inputPipe;
	Pipe &outputPipe;
	int runLength;	

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};
class RecordWrapper
{
	public:
	Record record;
	OrderMaker* sortOrder;
	RecordWrapper(Record *record,OrderMaker* orderMaker);
	static int compareRecords(const void *rw1, const void *rw2);
};
class ComparisonClass
{
	ComparisonEngine* compEngine;
	public:
	ComparisonClass();
	bool operator()(const pair<Record*,int> &lhs, const pair<Record*,int> &rhs);
};
void* workerFunc(void *bigQ);
void sortAndCopyToFile(vector<RecordWrapper*>& list,File* file);
void createRuns(int runlen, Pipe& in, File* file,OrderMaker& sortOrder);
void writeRunToFile(File* file, vector<Record*> &list);
void mergeRunsFromFile(File* file, int runLength,Pipe& out,OrderMaker& orderMaker);
void copyRecordsToFile(Page pages[],File* file,int runlen);
#endif
