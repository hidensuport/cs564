/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {}

void BufMgr::allocBuf(FrameId& frame) {
  bufMgr::advanceClock();
  if(bufDescTable[clockHand].valid == false){
    bufDescTable[clockHand].Set();
    frame = clockHand;
    return;
  }
  if(bufDescTable[clockHand].refbit == true){
    bufDescTable[clockHand].refbit = false;
    bufMgr::allocBuf(frame);
  }
  if(bufDescTable[clockHand].pinCnt > 0){
    bufMgr::allocBuf(frame);
  }
  if(bufDescTable[clockHand].dirty == true){
    bufDescTable[clockHand].file.flushFile();
    bufDescTable[clockHand].Set();
    frame = clockHand;
    return;
  }
  if(bufDescTable[clockHand].dirty == false){
    bufDescTable[clockHand].Set()
    frame = clockHand;
    return;
  }
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {

 FrameId framenum;
  try{
    this->hashTable.lookup( file, pageNo,  framenum);
    bufDescTable.at(framenum).pinCnt+=1;
    bufDescTable.at(framenum).refbit= true;
    page = &bufPool.at(framenum);
    return;
  }
  catch(HashNotFoundException e)
    {
        // if the page is not yet existed in the hashtable, allocate it and
        // set the bufDescTable
        // also, return the ptr to the page and its pageNo
        FrameId frameFree;
        allocBuf(frameFree);
        bufPool.at(frameFree) = file.readPage(pageNo);
        this->hashTable.insert(file, pageNo, frameFree);
        bufDescTable.at(frameFree).Set(file, pageNo);
        page = &bufPool[frameFree];
    }
  
  
  
  
  
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId framenum;
    try
    {
        // check if the page exist in the frame pool
        hashTable.lookup(file, pageNo, framenum);
       
        if(bufDescTable.at(framenum).pinCnt > 0) {
            bufDescTable.at(framenum).pinCnt-= 1;
            if(dirty == true) {
                bufDescTable.at(framenum).dirty = true;
            }
            return;
        }
        // if not pinned, throw exception
        else if(bufDescTable.at(framenum).pinCnt == 0){
            throw PageNotPinnedException(file.filename(), bufDescTable.at(framenum).pageNo, framenum);
        }
    }
    catch(HashNotFoundException e)
    {
    }
}


/**
 * @brief This function allocates a new empty page in our given file
 * and is assigned accordingly to our buffer pool 
 * 
 * @param file  Our File object
 * @param pageNo  Our page number assigned to our page in file
 * @param page  This is our reference to our page pointer
 */
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {



  FrameId frameNumber;

  allocBuf(frameNumber);

  bufPool[frameNumber] = file.allocatePage();

  page = &bufPool[frameNumber];

  pageNo = page->page_number();

  hashTable.insert(file, pageNo, frameNumber);

  bufDescTable[frameNumber].Set(file, pageNo);


}



void BufMgr::flushFile(File& file) {

  for(unsigned int i = 0; i < numBufs; i++) {

    if(bufDescTable[i].valid == false && bufDescTable[i].file == file) {
      throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
    }

    if(bufDescTable[i].file == file && bufDescTable[i].pinCnt > 0) {
      throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
    }


    if(bufDescTable[i].valid == true && bufDescTable[i].file == file) {

      if(bufDescTable[i].dirty == true) {
  bufDescTable[i].file.writePage(bufPool[i]);
  bufDescTable[i].dirty = false;

      }

      hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo);

      bufDescTable[i].clear();

    }

  }
}


/**
 * @brief This function deletes a page from a given filee
 * @param file  Object of file
 * @param PageNo  Page Number 
 */
void BufMgr::disposePage(File& file, const PageId PageNo) {


  FrameId frameNo;

  try {

    hashTable.lookup(file, PageNo, frameNo);

    if (bufDescTable[frameNo].pinCnt != 0) {

      throw PagePinnedException(bufDescTable[frameNo].file.filename(), bufDescTable[frameNo].pageNo, bufDescTable[frameNo].frameNo);

      

    }

    bufDescTable[frameNo].clear();
    hashTable.remove(file, PageNo);
    file.deletePage(PageNo);
    return;

  } catch(HashNotFoundException& e) {

    file.deletePage(PageNo);
    
  }
				  

}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
