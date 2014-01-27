#ifndef BUFFER_H
#define BUFFER_H

#include "TwoWayList.h"
#include "Record.h"
#include "File.h"
#include<map>
#include<list>

//TBD whether to place in definitions??? 
enum PageMode { read, write };

class Buffer
{
	private:
		//TwoWayList<Page>* bufferList;                 //Free list for buffers
		list<Page*> readHead;                     //header for list of read buffers.
		list<Page*> writeHead;                    //header for list of write buffers.

		struct PageInfo
		{
			bool isDirty;
			int page_num;
			Page* page;
			PageInfo(bool isDirty,int page_num,Page *page)
			{
				this->isDirty = isDirty;
				this->page_num = page_num;
				this->page = page;
			}
		};

		std::map<off_t,PageInfo> pageTable;                   //map for lookup if Page exists in Buffer.

		int numPageUsed;				  //Total Number of Pages in Buffers.
		const int BUFFER_SIZE = 8;

		//Function to check if the page specified already exists
		bool isPageExists(off_t pageNum);

		//Function to check if the Buffer is full
		bool isBufferFull();

		//Utility Function returns new page.
		Page* getNewPage(File *file);

		//Function to clean Buffer.
		void cleanBuffer();

		//Function to add new record to end of Page.

		int addToEnd(Record* record);
		void addPageInfo(bool isDirty,off_t page_num,Page *page);
		off_t getPageNumber(off_t page_num);
	public:
		Buffer();

		//method to add record to file.
		int add(File* file, Record* record, off_t num_records);

		//method to empty buffer to disk from pages in memory.Will write all pages in memory marked dirty to memory.
		void writeBuffer(File *file);

		//method to read a new record in record and return status 1 if success and 0 if failure
		int readRecord(File* file,Record* record,off_t num_records);	  

		//method to return a page from memory or disk.
		Page * getPage(File *file,off_t num_records);
};

#endif
