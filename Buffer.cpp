#include "Buffer.h"

Buffer::Buffer()
{
	numPageUsed = 0;
}
/**
 * @method isBufferFull to check if all the pages in buffer has been exhausted.
 */
bool Buffer::isBufferFull()
{
	return (BUFFER_SIZE == numPageUsed);
}
/**
 * @method isPageExists to check if a Page specified exists in Buffer.
 * @int page number to look.
 * @returns boolean true if page exists in Buffer else false.
 */
bool Buffer::isPageExists(off_t pageNum)
{
	return (pageTable.find(pageNum) != pageTable.end());
}
/**
 * Function utility which returns a new Page to the calling function. Function checks if the buffer is full then it empties the buffer by
 * calling writeBuffer and then allocates new Page.
 * @returns Pointer to page which can be used by the calling function.
 */
Page* Buffer::getNewPage(File *file)
{
	if(isBufferFull())
	{
		writeBuffer(file);
	}
	Page* newPage = new Page();
	numPageUsed++;
	return newPage;
}
/**
 * @method to set all buffer to null and set size used to 0.
 */
void Buffer::cleanBuffer()
{
	readHead.clear();
	writeHead.clear();
	numPageUsed = 0;
	pageTable.clear();
}
/**
 * @method add to add a record to the end of file. First it tries to add a record to end of current page 
 * @param File object of File class holding the reference to the file to which record needs to be written.
 * @param Record object to be written to File.
 *
 */
int Buffer::add(File* file,Record* record,off_t num_records)
{
	//determine page number;						??????	
	int page_num = num_records/PAGE_SIZE;
	int status = 0;
	if(writeHead.empty())
	{
		//read last page of file in write head and try adding in this.
		Page * page = getPage(file,page_num);
		writeHead.push_back(page);
		addPageInfo(true,page_num,page);
	        status = page->Append(record);	
	}
	else				//if buffer is not empty try adding record on last page inserted.
	{
		//if can be written on the last page write it!
		Page *page = writeHead.back();
		status = page->Append(record);
	}
	//get last page from list and add a new record to that page. if success good enough else try with new page.
	if(status == 0)
	{
		Page* newPage = getNewPage(file);

		//to be determined how to get proper page number for the page.
		off_t pageIndex = (file->GetLength());
		addPageInfo(true,pageIndex,newPage);
		writeHead.push_back(newPage);
		status = newPage->Append(record);
	}
	return status;
}
/**
 * @method addPageInfo to add Page information brought in memory to map.
 */
void Buffer::addPageInfo(bool isDirty,off_t page_num,Page* page)
{
	struct PageInfo pageInfo(isDirty,page_num,page);
	pageTable.insert(make_pair(page_num,pageInfo));
}
/**
 * @method getPage() invoked by add method to get a page where record needs to be added. Method aim is to look for the last page of the file
 * and return the same. It tries to access the same by first trying to check it in memory and if found removes it from read buffer and
 * returns the same page. Else if the page is not found the page is looked up in disk and instance of same is returned.
 * @param File *
 * @param off_t page_num
 * @returns Pointer to Page Instance
 */
Page* Buffer::getPage(File *file, off_t page_num)
{
	//check if the page exists in map if so extract it from memory else read from file
	if(isPageExists(page_num))
	{
		struct PageInfo temp  = pageTable.find(page_num)->second;

		if(!temp.isDirty)	//if page exists in read Buffer.
		{
			Page* page = temp.page;
			readHead.remove(page);
			return page;
		}
		//Page will not be in write buffer since this method is invoked when the write buffer is empty.
	}else
	{
		//if the file does not exists in memory read from file.
		Page *page;
		file->GetPage(page,page_num);
		return page;
	}
}
/**
 * @method writeBuffer to write all contents of buffer to disk. buffer is also cleaned and all heads are reinitialised to NULL.
 *
 */
void Buffer::writeBuffer(File *file)
{
	//Write Buffer to disk current length to be taken care
	off_t current_length = file->GetLength();
		
	//while there are Pages in write Buffer write all the buffer pages to file.
	while(!writeHead.empty())
	{
		Page* pageToWrite = writeHead.front();	
		file->AddPage(pageToWrite,current_length+1);
		current_length++;
		writeHead.pop_front();
	}
	//Once buffer is copied to disk clean the buffer in memory.
	cleanBuffer();
}
/**
 * @method readRecord to read next record either from Buffer or From disk.Method internally checks if requested record exist in 
 * buffer else reads the page with record from disk and store it in bUffer.
 * @param File
 * @param Record
 * @param num_records
 * @returns int value 1 from success and 0 for failure.
 */
int Buffer::readRecord(File* file, Record* record, off_t num_records)
{
	//if the requested page exists in memory read from map and return
	if(isPageExists(num_records))
	{
		Page *page = pageTable.find(num_records)->second.page;
		
		
	}else			//if requested record page does not exist in memory get a new page and read page from file and read record.
	{

	}
}
