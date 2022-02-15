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
  clockHand = (clockhand + 1)%(bufs - 1);
}

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

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

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
