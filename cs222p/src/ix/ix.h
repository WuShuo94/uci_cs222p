#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <stack>
#include <string>
#include <unordered_map>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);

    protected:
        IndexManager();
        ~IndexManager();

    public:
        short int composeKey(const Attribute &attribute, const void *key, const RID &rid, void *cKey);
        short int binSearch(const void *pageData, const Attribute &attribute, const void *conditionKey, short int cKeyLength);
        short int keyCompare(const Attribute &attribute, const void *conditionKey, const short int cKeyLength, const void *key, const short int keyLength);
        RC insertEntryInPage(void *pageData, const short int entrySlot, const void *entry, const short int entryLength);
        RC updateSlotDirectory(void *slotDirectory, const int entryslot, PageInfo pageInfo, const short int offset);
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, void *pageData, int depth, PageNum nextNodePage);

    private:
        static IndexManager *_index_manager;
};


class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    unsigned ixNumberOfPages;
    unsigned rootNodePage;
//    bool temp = false;

    string bindFileName;
    FILE *filePointer;


    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);
//  RC deletePage(PageNum pageNum);



};


class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();
        RC prepareIterator(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        bool highKeyNUll;


    private:
        IXFileHandle *ixfileHandle;
        bool lowKeyInclusive, highKeyInclusive;

        Attribute attribute;
        void *lowKey, *highKey, *pageData, *lowCompKey, *highCompKey;

        int a = 10;
        PageInfo pageInfo;
        short int nodeType;
        unsigned nextNodePage;
        RID i_rid, c_rid;
        short int lowKeyLength, highKeyLength;
        int rightNodePage;
        short int lowSlotID, highSlotID;
        bool scanFinished;

        IndexManager *ix;

};

#endif
