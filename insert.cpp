#include "catalog.h"
#include "query.h"
#include "index.h"
#include "datatypes.h"
#include <stdlib.h>
#include <stdio.h>
#include <cstring>

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{

    // Define the return value, length and attributes
    Status result; 
    int attrCntInfo;
    AttrDesc* attrs; 
    Status status2;
    status2 = OK;
    if(status2 != OK)
	return status2;
   
    result = attrCat->getRelInfo(relation, attrCntInfo, attrs);
    if (result != OK) {
    	return result;
    }
    
    // Define the record size
    AttrDesc curAttr;
    int size = 0;
    int i = 0;
    int j = 0;
   while(i<attrCntInfo) {
    	curAttr = attrs[i];
    	size += curAttr.attrLen;
	i++;
    }

    //allocate the size for the record
    Record rec;
    rec.length = size;
    rec.data = malloc(size); 
    
    
    attrInfo curInfo;
    while(j<attrCnt) {
    	curInfo = attrList[j];
    	
    	// Get the description of this attribute
    	AttrDesc desc;
    	result = attrCat->getInfo(relation, curInfo.attrName, desc);

	//The bug check, the following are the same
    	if (result != OK) {
    		return result;
    	}
    	// Use the memcpy function to copy the data
		memcpy(((char*)rec.data)+desc.attrOffset, curInfo.attrValue, desc.attrLen);
		j++;
	}
    		
    // Write the record in heapfile
    RID rid;
    HeapFile heap = HeapFile(relation, result);
    if (result != OK) {
    	return result;
    }
    heap.insertRecord(rec, rid);	
    	
	// Find which attrs are indexed
	for (int k=0;k<attrCnt;k++) {
    		curInfo = attrList[k];
    	
    		AttrDesc desc;
    		result = attrCat->getInfo(relation, curInfo.attrName, desc);//get the information of the attriibute
    		if (result != OK) {
    			return result;
    		}
    	
    		// finish the part with the insert of RID
    		if (desc.indexed) {
    			Index index = Index(curInfo.relName, desc.attrOffset, desc.attrLen, (Datatype)desc.attrType, 1, result);
    			if (result != OK) {
    				return result;
    			}
    			index.insertEntry(((char*)rec.data)+desc.attrOffset, rid);
    		}
	}
	
    return OK;
}
