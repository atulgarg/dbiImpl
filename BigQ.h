#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <queue>


using namespace std;

class BigQ {
public:
	priority_queue <Record*>* pqueue;
	pthread_t worker;
	OrderMaker* myOrder;
	File* sortedFile;
	Pipe &inputPipe;
	Pipe &outputPipe;
	Page* readCurrPage;
	int rLen;
		

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	void* workerFunc(void* sortWork);
};


#endif
