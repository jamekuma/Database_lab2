/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++)
    {
        bufDescTable[i].frameNo = i;
        bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // 把所有dirty page写入到磁盘上
    for (int i = 0; i < (int)numBufs; i++) {
        if (bufDescTable[i].valid && bufDescTable[i].dirty) {
            // flush page
            File* file = bufDescTable[i].file;
            file->writePage(bufPool[i]);
        }
    }

    // 释放堆上所有的空间
    delete []bufDescTable;
    delete []bufPool;
    delete hashTable;
}

void BufMgr::advanceClock() {
    // 更新时针
    clockHand++;
    clockHand %= numBufs;
}

void BufMgr::allocBuf(FrameId & frame) {
    FrameId pinnedCnt = 0;   // 计数被pin的刻度的数量
    do {
        advanceClock();
        // 该frame没有存page, 直接可用
        if (!bufDescTable[clockHand].valid) {
            break;
        }

        // 若refbit为true, 置空, 时钟更新
        if (bufDescTable[clockHand].refbit) {
            bufDescTable[clockHand].refbit = false;
            continue;
        }

        // 若被pin了, 则被pin的刻度的数量+1
        if (bufDescTable[clockHand].pinCnt) {
            pinnedCnt++;
            // 如果所有的frame都被pin了, 抛出异常
            if (pinnedCnt == numBufs) {
                throw BufferExceededException();
            }
        }
        // 否则该frame可用
        else {
            break;
        }
    } while (true);


    if (bufDescTable[clockHand].valid) {
        File* file = bufDescTable[clockHand].file;
        PageId pageNo = bufDescTable[clockHand].pageNo;
        if (bufDescTable[clockHand].dirty) {
            // flush Page
            file->writePage(bufPool[clockHand]);
        }

        // 清除HashTbl中的对应项
        hashTable->remove(file, pageNo);
    }
    bufDescTable[clockHand].Clear();
    frame = clockHand;
}


void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    try {
        FrameId targetFrame;
        hashTable->lookup(file, pageNo, targetFrame);
        bufDescTable[targetFrame].refbit = true;
        bufDescTable[targetFrame].pinCnt++;
        page = bufPool + targetFrame;  // 返回page指针
        return;
    }
    // 对应page不在pool中
    catch (HashNotFoundException e) {
        FrameId frameNo;
        allocBuf(frameNo);   // 分配一个frame
        bufPool[frameNo] = file->readPage(pageNo);  // 从文件向pool中读入一个page
        hashTable->insert(file, pageNo, frameNo);  // 插入HashTbl
        bufDescTable[frameNo].Set(file, pageNo);  // 设置frame状态
        page = bufPool + frameNo;        // 返回page指针
    }
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
    try {
        FrameId targetFrame;
        hashTable->lookup(file, pageNo, targetFrame);
        if (bufDescTable[targetFrame].pinCnt == 0) {
            throw PageNotPinnedException("", pageNo, targetFrame);
        }
        bufDescTable[targetFrame].pinCnt--;
        if (dirty) {
            bufDescTable[targetFrame].dirty = true;
        }
    }
    catch (HashNotFoundException e) {
        return;
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
    // 分配一个Page
    Page newPage = file->allocatePage();
    PageId newPageNo = newPage.page_number();

    // 分配一个frame
    FrameId frameNo;
    allocBuf(frameNo);

    // 将Page填入frame, 设置descTbl、HashTable
    bufPool[frameNo] = newPage;
    hashTable->insert(file, newPageNo, frameNo);
    bufDescTable[frameNo].Set(file, newPageNo);

    // return
    pageNo = newPageNo;
    page = bufPool + frameNo;
}

void BufMgr::disposePage(File* file, const PageId PageNo) {
    try {
        // 在hash中查找对应的frameNo
        FrameId frameNo;
        hashTable->lookup(file, PageNo, frameNo);

        // 清除bufDescTable、HashTbl中的项
        bufDescTable[frameNo].Clear();
        hashTable->remove(file, PageNo);
    }
    // 如果Pool中没有, 则不操作
    catch (HashNotFoundException e) {
    }

    file->deletePage(PageNo);
}

void BufMgr::flushFile(File* file) {
    // 查看每个frame
    for (int i = 0; i < (int)numBufs; i++) {
        if (bufDescTable[i].file == file) {
			if (bufDescTable[i].pinCnt) {
                throw PagePinnedException("", bufDescTable[i].pageNo, i);
            }
            if (!bufDescTable[i].valid) {
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }

            if (bufDescTable[i].dirty) {
                file->writePage(bufPool[i]);
            }
            hashTable->remove(file, bufDescTable[i].pageNo);
            bufDescTable[i].Clear();
        }
    }
}

void BufMgr::printSelf(void)
{
    BufDesc* tmpbuf;
    int validFrames = 0;

    for (std::uint32_t i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufDescTable[i]);
        std::cout << "FrameNo:" << i << " ";
        tmpbuf->Print();

        if (tmpbuf->valid == true)
            validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
