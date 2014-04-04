#include "RelOp.h"
#include <cstdlib>
/**
 * @method Run of Class SelectFile to read input records from DBFile and output to output Pipe.
 * @param DBFile input file
 * @param Pipe  output pipe
 * @param CNF
 * @param Record literal record
 *
 */
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) 
{
    this->inFile = &(inFile);
    this->outPipe = &(outPipe);
    this->selOp = &(selOp);
    this->literal = &(literal);
    //Start thread for internal sorting.
	int rc = pthread_create(&thread,NULL,runSelectFile,(void*)this);
	if(rc){
		cerr<<"Not able to create worker thread"<<endl;
		exit(1);
	}   
}
/**
 * @method WaitUntilDone to wait until thread completes for outputing data.
 *
 */
void SelectFile::WaitUntilDone () 
{
	pthread_join (thread, NULL);
}
/**
 * @method runSelectFile to select all records from input DBFile and output to Pipe and satisifies supplied CNF.
 *
 */
void* runSelectFile(void * sf)
{
    //read the initial values
    SelectFile* selectFile = (SelectFile*) sf;
    DBFile* inFile = selectFile->inFile;
    Pipe* outPipe = selectFile->outPipe;
    CNF* selOp = selectFile->selOp;
    Record* literal = selectFile->literal;
    ComparisonEngine compEngine; 
    //iterate through all records in file and check for CNF
    Record record;
    while(inFile->GetNext(record,*selOp,*literal) != 0)
    {
            outPipe->Insert(&record);
    }
    outPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) 
{
   this->inPipe = &(inPipe);
   this->outPipe = &(outPipe);
   this->selOp = &(selOp);
   this->literal = &(literal);
    //Start thread for internal sorting.
    int rc = pthread_create(&thread,NULL,runSelectFile,(void*)this);
    if(rc){
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    } 
}
/**
 * @method WaitUntilDone for SelectPipe waits for thread to complete, and then deletes the temporary object.
 */
void SelectPipe:: WaitUntilDone () 
{
    pthread_join (thread, NULL);
}
/**
 * @method runSelectPipe
 */
void* runSelectPipe(void * sf)
{
    //read the initial values
    SelectPipe* selectPipe = (SelectPipe*) sf;
    Pipe* inPipe = selectPipe->inPipe;
    Pipe* outPipe = selectPipe->outPipe;
    CNF* selOp = selectPipe->selOp;
    Record* literal = selectPipe->literal;
    ComparisonEngine compEngine; 
    //iterate through all records in file and check for CNF
    Record record;
    while(inPipe->Remove(&record) != 0)
    {
        if(compEngine.Compare (&record, literal, selOp) == 1) 
            outPipe->Insert(&record);
    }
    outPipe->ShutDown();
}
/**
 *
 *
 */
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
    this->inPipe = &(inPipe);
    this->outPipe = &(outPipe);
    this->keepMe = keepMe;
    this->numAttsInput = numAttsInput;
    this->numAttsOutput = numAttsOutput;

    //Start thread for internal sorting.
    int rc = pthread_create(&thread,NULL,runProject,(void*)this);
    if(rc)
    {
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    } 
}
/**
 *
 */

void Project::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}
/**
 *
 */

void * runProject(void *sf)
{
    Project* project = (Project*) sf;
    Pipe* inPipe = project->inPipe;
    Pipe*  outPipe = project->outPipe;
    int* keepMe = project->keepMe;
    int numAttsInput = project->numAttsInput;
    int numAttsOutput = project->numAttsOutput;
    //How to retain order as per keepme????
    Record record;
    while(inPipe->Remove(&record) != 0)
    {
        record.Project(keepMe,numAttsOutput,numAttsInput);
        outPipe->Insert(&record);
    }
    outPipe->ShutDown();
}

/**
 * @method WaitUntilDone to wait for Join thread to complete and clean up.
 *
 */
void Join::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}
/**
 *
 *
 */
void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
    this->inPipeL = &(inPipeL);
    this->inPipeR = &(inPipeR);
    this->outPipe = &(outPipe);
    this->selOp = &(selOp);
    this->literal = &(literal);
    //Start thread for internal sorting.
    int rc = pthread_create(&thread,NULL,runJoin,(void*)this);
    if(rc)
    {
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    }  
}
void Join::Use_n_Pages (int n)
{
    runlen = n;
}

/**
 * @method runJoin to Join two tables in thread by making adequate sort orders and run this method as a thread.
 * @param void* expects pointer to WrapperClass object.
 * @returns void*.
 *
 *
 */
void* runJoin(void *sf)
{
    Join * joinObj = (Join*) sf;
    
    Pipe* outPipe = joinObj->outPipe;
    int runLen = joinObj->runlen;
    CNF* selOp = joinObj->selOp;
    Pipe * inPipeL = joinObj->inPipeL;
    Pipe * inPipeR = joinObj->inPipeR;
    Record* literal = joinObj->literal;

    Pipe outPipeL(100);
    Pipe outPipeR(100);
    
    OrderMaker leftOrder;
    OrderMaker rightOrder;
    
    ComparisonEngine compEngine;
    Record lrecord;
    Record rrecord;
    Record* prevRecord = NULL;

    DBFile dbfile;
    int numAttsLeft;
    int numAttsRight;
    int *attsToKeep;
    int numAttsToKeep;
    int startOfRight;
    
    if(selOp->GetSortOrders(leftOrder,rightOrder) != 0)
    {
        BigQ bigqL(*inPipeL,outPipeL,leftOrder,runLen);
        BigQ bigqR(*inPipeR, outPipeR,rightOrder, runLen);
        //initialise lrecord and rrecord and change loops to while from do while.
        int stleft = outPipeL.Remove(&lrecord);
        int stright  = outPipeR.Remove(&rrecord);
        
        if(stleft!=0 && stright !=0)
        {
            numAttsLeft = lrecord.numAttributes();
            numAttsRight = rrecord.numAttributes();
            numAttsToKeep = numAttsLeft + numAttsRight;

            attsToKeep = new int[numAttsToKeep];
            
            for(int i=0;i<numAttsLeft;i++)
             attsToKeep[i] = i;
            startOfRight = numAttsLeft;
            for(int i=0;i<numAttsRight;i++)
                attsToKeep[i+numAttsLeft] = i;
        }
        else
        {
            cerr<<"One of the pipes was empty"<<endl;
            outPipe->ShutDown();
            return NULL;
        }

        while( stleft!=0 && stright !=0)
        {
            //if previous record was same as this record read records from dbfile.
            if(prevRecord == NULL || compEngine.Compare(prevRecord,&lrecord, selOp) == 0)
            {
                dbfile.Create("ntsdhdhfkzgtsesdbynyfrxrfy",heap,NULL); 
 
                //remove left record while left record is less than right record.
                while(compEngine.Compare(&lrecord, &leftOrder, &rrecord, &rightOrder) < 0 && (stleft = outPipeL.Remove(&lrecord))!=0);

                //remove right record while left record is greater than left record.
                while(compEngine.Compare(&lrecord, &leftOrder, &rrecord, &rightOrder) > 0 && (stright = outPipeR.Remove(&rrecord))!=0);

                //while record are equal
                do
                {
                    Record mr;
                    Record tempCopy;
                    tempCopy.Copy(&rrecord);
                    dbfile.Add(tempCopy);
                    mr.MergeRecords(&lrecord, &rrecord, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep,startOfRight);
                    addRecordToPipe(mr, outPipe, selOp, literal, compEngine);

                }while((stright = outPipeR.Remove(&rrecord)) && compEngine.Compare(&lrecord, &leftOrder, &rrecord, &rightOrder) == 0);

                dbfile.MoveFirst();
                dbfile.Close();
            }
            else //else read record from pipe and write it to dbfile instance after clearing.
            {
                dbfile.Open("ntsdhdhfkzgtsesdbynyfrxrfy");
                while(dbfile.GetNext(rrecord) != 0 && compEngine.Compare(&lrecord, &leftOrder, &rrecord, &rightOrder) == 0)
                {
                    Record mr;
                    mr.MergeRecords(&lrecord, &rrecord, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep,startOfRight);
                    addRecordToPipe(mr, outPipe, selOp, literal, compEngine);
                }
                dbfile.MoveFirst();
            }
            if(prevRecord == NULL)
                prevRecord = new Record();
            prevRecord->Copy(&lrecord);
        }
    }
    else
    {
        Record tempRecord;
        bool flag = true;
        //block nested join.
        while(inPipeL->Remove(&lrecord)!=0)
        {
            if(flag)
            {
                Record temp;
                if(inPipeR->Remove(&rrecord)!=0)
                {
                    //initialise all once
                    numAttsLeft = lrecord.numAttributes();
                    numAttsRight = rrecord.numAttributes();
                    numAttsToKeep = numAttsLeft + numAttsRight;

                    attsToKeep = new int[numAttsToKeep];

                    for(int i=0;i<numAttsLeft;i++)
                        attsToKeep[i] = i;
                    startOfRight = numAttsLeft;
                    for(int i=0;i<numAttsRight;i++)
                        attsToKeep[i+numAttsLeft] = i;

                    do
                    {
                        Record mr;
                        temp.Copy(&rrecord);
                        mr.MergeRecords(&lrecord,&rrecord, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep,startOfRight);
                        addRecordToPipe(mr, outPipe, selOp, literal, compEngine);
                        dbfile.Add(temp);
                        dbfile.MoveFirst();
                        flag = false;
                    }while(inPipeR->Remove(&rrecord)!=0);
                }
            }else
            {
                while(dbfile.GetNext(tempRecord)!=0)
                {
                    Record mr;
                    mr.MergeRecords(&lrecord,&tempRecord, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep,startOfRight);
                    addRecordToPipe(mr, outPipe, selOp, literal, compEngine);
                }
                dbfile.MoveFirst();
            }

        }

    }
    dbfile.Close();
    remove("ntsdhdhfkzgtsesdbynyfrxrfy");
    remove("ntsdhdhfkzgtsesdbynyfrxrfy.meta");
    outPipe->ShutDown();
}
/*
 * @method addRecordToPipe utility function to add records which qualify specified cnf to specified output
 * pipe.
 * @param Record instance to be added.
 * @param Pipe instance to which records need to be added.
 * @param CNF which record instance need to qualify.
 * @param Record literal used for comparison.
 * @param ComparisonEngine to be used for comparison.
 */
void addRecordToPipe(Record& record, Pipe* outPipe, CNF* cnf, Record* literal,ComparisonEngine&  compEngine)
{
        outPipe->Insert(&record);
}
/**
 * @method Run for Duplicate removal. Method creates a thread for removing duplicate tuples.
 */
void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
    this->inPipe = &(inPipe);
    this->outPipe = &(outPipe);
    this->mySchema = &(mySchema);
    //Start thread for internal sorting.
    int rc = pthread_create(&thread,NULL,runDuplicateRemoval,(void*)this);
    if(rc)
    {
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    }
}
/**
 * @method runDuplicateRemoval to remove duplicates from input records specified from input pipe in 
 * DuplicateRemoval class object.
 * @param void* of type pointer pointing to DuplicateRemoval class instance
 * @returns void* NULL in case of failure.
 */
void * runDuplicateRemoval(void * runDuplicate)
{
    DuplicateRemoval* duplicateRemovalObj = (DuplicateRemoval*) runDuplicate;
    Pipe* outPipe = duplicateRemovalObj->outPipe;
    Schema *mySchema = duplicateRemovalObj->mySchema;
    Pipe out(100);
    //Initialise OrderMaker object with all the attributes.
    OrderMaker sortOrder(mySchema);
    ComparisonEngine compEng;
    BigQ bigq(*(duplicateRemovalObj->inPipe),out,sortOrder,duplicateRemovalObj->runlen);
    //Take the output from outPipe and remove duplicates;
    Record prevRecord;
    Record tempRecord;
    Record record;
    int status = out.Remove(&prevRecord);

    if(status != 0)
    {
       tempRecord.Copy(&prevRecord);
       outPipe->Insert(&tempRecord);
       while(out.Remove(&record)!=0)
        {
            if(compEng.Compare(&record,&prevRecord,&sortOrder) != 0) 
            {
                prevRecord.Copy(&record);
                outPipe->Insert(&record);
            }
        }
    }
   outPipe->ShutDown();                      
}
/**
 * @method Use_n_Pages to initialsie runLength Value.
 */
void DuplicateRemoval::Use_n_Pages (int n)
{
    runlen = n;
}
/**
 * @method WaitUntilDone to wait for thread removing duplicates completes and does clean up.
 */
void DuplicateRemoval::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}
/**
 * @method Run of WriteOut object to start a new thread instance to write records of input pipe
 * to output pipe.
 * @param Pipe instance to read input records.
 * @param FILE File instance to write out records. method expects this instance to be initialised with open file.
 * @param Schema object for records to read.
 *
 */
void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
    this->inPipe = &(inPipe);
    this->outFile = (outFile);
    this->mySchema = &(mySchema);
    //Start thread for internal sorting.
    int rc = pthread_create(&thread, NULL, runWriteOut,(void*)this);
    if(rc)
    {
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    }
}
/**
 * @method WaitUntilDone to wait for writeout thread to complete.
 */
void WriteOut::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}
/**
 * @method runWriteOut to write records to specified file. Method expects pointer to 
 * class WriteOut object. Method runs as separate thread.
 * @param void* pointing to WriteOut class object.
 * @returns void* NULL in case of failure.
 *
 */
void* runWriteOut(void * writeOb)
{
    
    WriteOut* writeObject = (WriteOut*)writeOb;
    Pipe* inPipe = writeObject->inPipe;
    Schema* mySchema = writeObject->mySchema;
    FILE *outFile = writeObject->outFile;
    
    Record record;
    while(inPipe->Remove(&record) != 0)
    {
        record.PrintToFile(mySchema, outFile);
    }
}
/**
 * @method Run to start a new thread to compute sum over records specified in input pipe.
 * @param inPipe reference to pipe to read input records from.
 * @param outPipe reference to pipe to write output records.
 * @param Function reference to function to perform aggregation.
 *
 */
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
    this->inPipe = &(inPipe);
    this->outPipe = &(outPipe);
    this->computeMe = &(computeMe);
    //Start thread for internal sorting.
    IA = {"int", Int};
    DA = {"double", Double};

    int rc = pthread_create(&thread,NULL, RunSum,(void*)this);
    if(rc)
    {
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    }    
}
/**
 * @method WaitUntilDone, class method to wait for thread computing Sum to complete.
 */
void Sum::WaitUntilDone ()
{
    pthread_join (thread, NULL);
}
/**
 * @method RunSum to run as separate thread and compute sum for records specified in input pipe. Method expects a input parameter
 * as instance of Sum class.
 * @param void* pointer to Sum class object.
 * @returns void* NULL in case of failure.
 * 
 */
void* RunSum(void * sr)
{
    Sum* sum = (Sum*) sr;
    Pipe* inPipe = sum->inPipe;
    Pipe*  outPipe = sum->outPipe;
    Function* computeMe = sum->computeMe;
    
    Record record;
    int IntResult;
    double DoubleResult;
    double finalDoubleSum = 0.0;
    Type type;
    while(inPipe->Remove(&record) != 0)
    {
        type = computeMe->Apply(record,IntResult,DoubleResult); 
        finalDoubleSum+=(DoubleResult+IntResult);
        IntResult = DoubleResult = 0;
    }

    Record sumRecord = constructSumRecord(type, finalDoubleSum);
    outPipe->Insert(&sumRecord);
    outPipe->ShutDown();   
}
/**
 * @method Run of GroupBy class invokes another thread to run group by.
 * @param inPipe pipe instance for reading input records.
 * @param outPipe pipe instance for writting output records.
 * @param OrderMaker instance to use for grouping.
 * @param Function to compute aggregation over group
 *
 */
void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
    this->inPipe = &(inPipe);
    this->outPipe = &(outPipe);
    this->groupAtts = &(groupAtts);
    this->computeMe = &(computeMe);
    //Start thread for internal sorting.
    int rc = pthread_create(&thread,NULL, RunGroupBy,(void*)this);
    if(rc)
    {
        cerr<<"Not able to create worker thread"<<endl;
        exit(1);
    }
}
/**
 * Function to wait for thread running Group by operation to complete.
 */
void GroupBy::WaitUntilDone()
{
    pthread_join (thread, NULL);
}
/**
 * Function to initialise runlength of pages for groupby operation.
 * @param int value for runlength.
 */
void GroupBy::Use_n_Pages(int n)
{
    runlen = n;
}
/**
 * @method RunGroupBy to run group by operation as a separate thread. Method expects GroupBy class Object as input parameter and returns
 * NULL in case of failure.
 * @param void * pointer pointing to GroupBy object.
 * @returns void* NULL in case of failure.
 */
void* RunGroupBy(void *gb)
{
    GroupBy* groupby =(GroupBy*)gb;
    Pipe* inPipe = groupby->inPipe;
    Pipe* outPipe = groupby->outPipe;
    OrderMaker* groupAtts = groupby->groupAtts;
    Function* computeMe = groupby->computeMe;
    Pipe midPipe(100);

    BigQ bigq(*inPipe, midPipe, *groupAtts, groupby->runlen);
    
    Record prevRecord,record;
    ComparisonEngine compEngine;
    int IntResult = 0;
    double DoubleResult = 0;
    double finalResult = 0.0; 
    
    Type type;
    int status = midPipe.Remove(&prevRecord);
    int numAtts = 1 + groupAtts->numAtts;
    
    int* attsToKeep = new int[numAtts];
    attsToKeep[0] = 0;
    for(int i=1;i< numAtts;i++)
        attsToKeep[i] = groupAtts->whichAtts[i-1];
    
    while(status != 0)
    {
        type = computeMe->Apply(prevRecord,IntResult,DoubleResult);
        finalResult+=(IntResult+DoubleResult);
        DoubleResult = IntResult = 0;
        
        status = midPipe.Remove(&record);        
        if(status!=0 && compEngine.Compare(&prevRecord, &record, groupAtts) != 0)
        {
            Record sumRecord = constructSumRecord(type, finalResult);
            Record tempRecord;
            tempRecord.MergeRecords(&sumRecord, &prevRecord, 1, prevRecord.numAttributes(), attsToKeep, numAtts, 1);
            outPipe->Insert(&tempRecord);
            finalResult = 0.0;
        }
        prevRecord.Copy(&record);
    }
    Record sumRecord = constructSumRecord(type, finalResult);
    Record tempRecord;
    tempRecord.MergeRecords(&sumRecord, &prevRecord, 1, prevRecord.numAttributes(), attsToKeep, numAtts,1);
    outPipe->Insert(&tempRecord);
    
    outPipe->ShutDown();
}
/**
 * @method constructSumRecord method utility to construct a record with schema defining attribute as type
 * parameter specified and value as passed in val.
 * @param type {Int, Double, String}
 * @param val
 * @returns Record instance with type as specified schema and value.
 */
Record constructSumRecord(Type type, double val)
{
    Record sumRecord;
    Schema *sch;
    Attribute att;
    char buffer[50];
    if(type == Int)
    {
        att.name = "Int";
        att.myType = Int;
        sch = new Schema("sum",1, &att);
        sprintf(buffer, "%d|",(int) val);
    }
    else{
        att.name = "Double";
        att.myType = Double;
        sch = new Schema("sum",1, &att);
        //to check for decimal places to print in double sum.
        sprintf(buffer, "%g|", val);
    }
    sumRecord.ComposeRecord(sch, buffer);
    return sumRecord;
}
