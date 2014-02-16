#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): inputPipe(in),outputPipe(out),sortOrder(sortorder) {
	// read data from in pipe sort them into runlen pages
	runLength = runlen;
	int rc = pthread_create(&worker,NULL,workerFunc,(void*)this);
	if(rc){
		cerr<<"Not able to create worker thread"<<endl;
		exit(1);
	}
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	//out.ShutDown ();
}

BigQ::~BigQ () {
}
/*
 *
 */
void* workerFunc(void *bigQ)
{
	BigQ *bq  = (BigQ*) bigQ;
	sortOrder = bq->sortOrder;
	File* file = new File();
	file->Open(0,"temp.dat");
	Pipe& in = bq->inputPipe;
	Pipe& out = bq->outputPipe;
	int runlen = bq->runLength;
	//create file of sorted runs.
	createRuns(runlen,in,out,file);
	cout<<"File Length "<<file->GetLength()<<"\n";
	//once a file is created of sorted runs merge each of the run.
	//mergeRunsFromFile(file,runlen,out,sortOrder);
	file->Close();
	out.ShutDown ();

}
/**
 *
 */
int comparator(Record* r1,Record* r2)
{
	ComparisonEngine* compEngine = new ComparisonEngine();
	return (compEngine->Compare(r1,r2,&sortOrder) <= 0);
}
/**
 * @method createRuns to create a file of sorted runs and number of runs.
 * @returns total number of runs created.
 *
 */
void createRuns(int runlen,Pipe& in,Pipe& out,File *file)
{
	Record* currentRecord = new Record();
	Page* pages= new Page[runlen]();
	vector<Record*> list;
	int numPages = 0;
	while(in.Remove(currentRecord) != 0)
	{
		//push record to list and put the same to page to keep a check of number of pages to compare with run length.
		//if page was full
		cout<<"Yahan "<<endl;
		if(pages[numPages].Append(currentRecord) == 0)
		{
			//Page Full
			if(numPages<runlen)
				numPages++;
			else
			{
			   //get all records from array of pages and put it to vector to sort and put it to file.
			   copyRecordsToFile(pages,file,runlen-1);
			   numPages = 0;
			   pages = new Page[runlen]();
			}
			pages[numPages].Append(currentRecord);	
		}
		
		currentRecord = new Record();
	}
	//If records in list are less than page.
	copyRecordsToFile(pages,file,numPages);
}
void copyRecordsToFile(Page pages[],File* file,int runlen)
{
	vector<Record*> list;
	cout<<"Run len : "<<runlen<<"\n";
	for(int i=0;i<=runlen;i++)
	{
		Record * record = new Record();
		while(pages[i].GetFirst(record)!=0)
		{
			list.push_back(record);
			record = new Record();
		}
	}
	sort(list.begin(),list.end(),comparator);
	writeRunToFile(file,list);
}
/**
 * @method writeRunToFile to write records read from input pipe to File as sorted runs.
 * @param File* pointer to File where records need to be written.
 * @param vector<Record> list of Records to be written to file.
 * 
 */
void writeRunToFile(File* file, vector<Record*> &list)
{
	cout<<"WriteRunToFile : " + list.size()<<"\n";
	Page* page = new Page();
	for(int i=0;i<list.size();i++)
	{
		cout<<"Iterating "<<list.size()<<" "<<i<<"\n";
		Record* record = list[i];
		int status = page->Append(record);
		cout<<"Status :: "<<status<<"\n";
		//if record was not added to page i.e. page was full.
		if(status == 0)
		{
			off_t offSet = file->GetLength();
			file->AddPage(page,offSet);
			page->EmptyItOut();
			//append the record to new page.
			page->Append(record);
		}
	}
	cout<<"ATul Garg\n";
	off_t offSet = file->GetLength();
	cout<<"offset : "<<offSet<<"\n";
	file->AddPage(page,offSet);
	page->EmptyItOut();
}
class ComparisonClass
{
	ComparisonEngine* compEngine;
	public:
	ComparisonClass()
	{
		compEngine = new ComparisonEngine();
	}
	bool operator()(const pair<Record,int> &lhs, const pair<Record,int> &rhs)
	{
		Record r1 = lhs.first;
		Record r2 = rhs.first;
		return (compEngine->Compare(&r1,&r2,&sortOrder) <= 0);
	}
};
/**
 * 
 */
void mergeRunsFromFile(File* file, int runLength,Pipe& out,OrderMaker& orderMaker)
{
	int fileLength = file->GetLength();
	cout<<"File Length : "<<fileLength<<"\n";
	int numRuns = ceil(fileLength*1.0f/runLength);
	std::priority_queue<pair<Record,int>, std::vector<pair<Record,int> >,ComparisonClass> priorityQueue;
	//Array of Pages to keep hold of current Page from each of run.
	Page* pageBuffers = new Page[runLength]();
	//initialise each page with corresponding page in File.
	vector<off_t> offset;
	//initialise offset array to keep track of next page for each run.
	//Initialise each of the Page Buffers with the first page of each run.
	int k =0;
	for(int i=0;i<=fileLength;i+=runLength)
		offset.push_back(i);
	for(int i=0;i<=fileLength;i+=runLength)
	{
		//Get the Page for offset in Buffer.
		file->GetPage(&pageBuffers[k],offset[k]);
		//increament offset for run
		offset[k] = offset[k] + 1;
		//For each of the page get the first record in priority queue.
		Record record;
		pageBuffers[k].GetFirst(&record);
		priorityQueue.push(make_pair(record,k));
		k= k+1;
	}
	//while priority queue is not empty keeping popping records from Priority Queue and write it to pipe.
	while(!priorityQueue.empty())
	{
		pair<Record, int> topPair = priorityQueue.top();
		Record record = topPair.first;
		int runNum = topPair.second;
		priorityQueue.pop();
		//Write record to output pipe.
		out.Insert(&record);
		//Get the next record from Record Num and add it to priorityQueue.
		Record recordToInsert;
		if(pageBuffers[runNum].GetFirst(&recordToInsert) == 0)
		{
			//if no records were found from the page try to get the next page if page exists in the same run.
			off_t currOffset = offset[runNum];
			//only if there are more pages in run read the next page else skip.
			if(currOffset%runLength != 0 || currOffset <fileLength)
			{	
				file->GetPage(&pageBuffers[runNum],currOffset);
				//increament offset after reading page from current offset for run
				offset[runNum]++;	
				
			}
			else
			{
				//No more Pages to read
				continue;
			}
		}//no else block required since it has no more pages to read.
		priorityQueue.push(make_pair(recordToInsert,runNum));
	}
}
