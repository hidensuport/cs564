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

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1)%(bufDescTable.size()); //Advances Clock and Rolls back over to zero
}

void BufMgr::allocBuf(FrameId& frame) {
  int validRemaining = bufDescTable.size();
  int cases = 1;
  bool foundNoPin = false;
  for(unsigned int i = 0; i < bufDescTable.size(); i++){ //Checks to make sure that is at least 1 unpined page else throws a BufferExceededException
    if(bufDescTable.at(i).pinCnt == 0){
      foundNoPin = true;
    }
  }
  if(!foundNoPin){
    throw BufferExceededException();
  }
  while(true){ 
    switch(cases){
      case(1): //Advances Clock and Checks if all pinCnts are 1
        advanceClock();
        if(bufDescTable.at(clockHand).pinCnt > 0){
          validRemaining -= 1;
        }
        cases = 2;
        break;
      case(2):
        if(bufDescTable.at(clockHand).valid == true){ //Checks Valid, if valid move on to checking refbit
          cases = 3;
          break;
        }
        else{ //Wasn't Valid Leaves to call function to set the frame
          frame = clockHand;
          return;
        }
      case(3):
        if(bufDescTable.at(clockHand).refbit == true){ //If refbit was true, sets to false and sends back to start on next clock hand
          bufDescTable.at(clockHand).refbit = false;
          cases = 1;
          break;
        }
        else{ //refbit was false so moves on to checking the pin count
          cases = 4;
          break;
        }
      case(4):
        if(bufDescTable.at(clockHand).pinCnt > 0){ //if pincount is > 0 then sends back to start to check on next clock hand
          cases = 1;
          break;
        }
        else{ //pincount was 0 so moves on to checking if dirty or not
          cases = 5;
          break;
        }
        case(5): //if dirty removes from the hashtable before returning the frame
          if(bufDescTable.at(clockHand).dirty == true){
            bufDescTable.at(clockHand).file.writePage(bufPool.at(clockHand));
            hashTable.remove(bufDescTable.at(clockHand).file, bufDescTable.at(clockHand).pageNo);    
            bufDescTable.at(clockHand).clear();       
            frame = clockHand;
            return;
          }
          else{ 
            frame = clockHand;
            return;
          }
    }

      
      
  }
  
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {

 FrameId framenum;
  try{
    this->hashTable.lookup( file, pageNo,  framenum);
    //use lookup call to find whether Page is in the buffer pool
    bufDescTable.at(framenum).pinCnt+=1;
    //Page is in the buffer pool. increment the pinCnt for the page parameter.
    bufDescTable.at(framenum).refbit= true;
    //set refbit to true
    page = &bufPool.at(framenum);
    //return a pointer to the frame
    return;
  }
  catch(HashNotFoundException e)
    {
        // if the page is not yet existed in the hashtable
        FrameId frameFree;
        allocBuf(frameFree);
  // allocate a buffer frame
        bufPool.at(frameFree) = file.readPage(pageNo);
     // set the bufDescTable
        this->hashTable.insert(file, pageNo, frameFree);
    //read the page from disk into the buffer pool frame
        bufDescTable.at(frameFree).Set(file, pageNo);
    // set the bufDescTable
        page = &bufPool[frameFree];
    // also, return the ptr to the page and its pageNo
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
    //Decrements the pinCnt of the frame containing
            if(dirty == true) {
    //sets the dirty bit if the dirty is true
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

  // Allocate a new buffer frame
  allocBuf(frameNumber);

  // New page is created at specified frame
  bufPool[frameNumber] = file.allocatePage();

  // Set page pointer to frame 
  page = &bufPool[frameNumber];

  // Set page number
  pageNo = page->page_number();

  // Insert file with page number and frame number into our hash table
  hashTable.insert(file, pageNo, frameNumber);

  // And set our frame in the buffer
  bufDescTable[frameNumber].Set(file, pageNo);


}


/** 
 * @brief This function writes out dirty pages and this function does
 * so accordingly when frames are unpinned, or else exception is thrown
 * @param file  Our File object
 */
void BufMgr::flushFile(File& file) {

  // Loop through our frame analyzing each page and first check whether
  // pages are valid and are unpinned. If conditions are satisfied for flushing
  // then flush to disk accordingly or throw error whenever page found is unpinned
  // or invalid
  for(unsigned int i = 0; i < numBufs; i++) {

    // Throw error when page is invalid
    if(bufDescTable[i].valid == false && bufDescTable[i].file == file) {
      throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
    }

    // Throw error when page is pinned or in other words pin count is greater than zero
    if(bufDescTable[i].file == file && bufDescTable[i].pinCnt > 0) {
      throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
    }


    // If our page is valid
    if(bufDescTable[i].valid == true && bufDescTable[i].file == file) {

      // If our page is dirty than flush it to disk
      if(bufDescTable[i].dirty == true) {
        bufDescTable[i].file.writePage(bufPool[i]);
        bufDescTable[i].dirty = false;

      }

      // Remove page from our hashtable
      hashTable.remove(bufDescTable[i].file, bufDescTable[i].pageNo);

      // Call clear to clear out our page frame
      bufDescTable[i].clear();

    }

  }
}


/**
 * @brief This function deletes a page from a given file
 * @param file  Object of file
 * @param PageNo  Page Number 
 */
void BufMgr::disposePage(File& file, const PageId PageNo) {


  FrameId frameNo;

  try {

    hashTable.lookup(file, PageNo, frameNo);

    // Throw error when page is pinend
    if (bufDescTable[frameNo].pinCnt != 0) {
      throw PagePinnedException(bufDescTable[frameNo].file.filename(), bufDescTable[frameNo].pageNo, bufDescTable[frameNo].frameNo);
    }

    // Otherwise clear our page frame and delete
    bufDescTable[frameNo].clear();
    hashTable.remove(file, PageNo);
    file.deletePage(PageNo);
    return;

  } catch(HashNotFoundException& e) {

    // Page was not found, so simply return
    
    return;
    
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
