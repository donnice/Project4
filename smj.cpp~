#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <vector>
#include <cstring>
#include <stdlib.h>
#include <stdio.h>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    	cout << "Algorithm: SM Join" << endl;
    	Status status;
	Status status1;
	Status status2;
	Status status3;

	//get descriptions for attributes of both relations
	AttrDesc* r1;
	AttrDesc* r2;
	status3 = OK;
	if (status3 != OK) 
		return status;
    
    	AttrDesc* rel;
    	int num;
    	int size1=0;
    	int size2=0;
	int size3 = 0;

    	status = attrCat->getRelInfo(result, num, rel);
    	if (status != OK) 
		return status;
    
    	//Define two rels
    	AttrDesc* rel1;
    	status1 = attrCat->getRelInfo(attrDesc1.relName, num, rel1);
    	if (status1 != OK) 
		return status; 

    	for (int i=0;i<num;i++) 
    		size1 += rel1[i].attrLen;
    
    
    	AttrDesc* rel2;
    	status = attrCat->getRelInfo(attrDesc2.relName, num, rel2);
    	if (status != OK) 
		return status;

   	 for (int i=0;i<num;i++) {
    		size2 += rel2[i].attrLen;
    	}
    
  
    
    	// Find useable buffer pages
    	int numPages = bufMgr->numUnpinnedPages();
    	int k = (int) ((float)(numPages*.8));
    
    	// Get size of output record
    	int size=0;
    	for (int i=0;i<projCnt;i++) {
        	size += attrDescArray[i].attrLen;
    	}
    
   
    	int maxTuples1 = k/2 * 1024 / size1;
    	int maxTuples2 = k/2 * 1024 / size2;
    	int Tuples1 = HeapFile(attrDesc1.relName, status).getRecCnt();
    	int Tuples2 = HeapFile(attrDesc2.relName, status).getRecCnt();
    
    	Record rec1;
    	Record rec2;
    
    	// Open sorted files
    	SortedFile file1 = SortedFile(attrDesc1.relName, attrDesc1.attrOffset, 
        	                    attrDesc1.attrLen, (Datatype)attrDesc1.attrType,
        	                    maxTuples1, status);
    	if (status != OK) 
		return status; 
    	SortedFile file2 = SortedFile(attrDesc2.relName, attrDesc2.attrOffset, 
        	                    attrDesc2.attrLen, (Datatype)attrDesc2.attrType,
        	                    maxTuples2, status);
    	if (status != OK) 
		return status;
    
    	// Open output heap
    	HeapFile heapRes = HeapFile(result, status);
    	if (status != OK) 
		return status; 
    
   	 status = file1.next(rec1);
   	 if (status != OK) 
		return status; 
   	 status = file2.next(rec2);
   	 if (status != OK) 
		return status;
    
	bool test = false;    

   	Tuples1--;
   	Tuples2--;
    	char* data1 = (char*)rec1.data;
    	char* data2 = (char*)rec2.data;
    	char* curData = data1;
    	Record curRec = rec1;
    	
    
    	std::vector<Record> relSubset1;
    	std::vector<Record> relSubset2;
    
        
    	for(;;){
    	    relSubset1.clear();
    	    relSubset2.clear();
        
    	    curData=data1;
    	    curRec = rec1;
        
    	   
    	    while (Operators::matchRec(rec1, rec2, attrDesc1, attrDesc2) < 0) {
           	 if (file1.next(rec1) != OK) 
			return OK;
           	 data1 = (char*)rec1.data;
           	 curData = data1;
           	 curRec = rec1;
            }
            
      
        relSubset1.push_back(rec1);
            
        // Insert rel1 values into relsubset until it differs
      	  for(;;){ 
        	    if (file1.next(rec1) != OK) 
			break; 
        	    data1 = (char*)rec1.data;
            
        	    if (Operators::matchRec(rec1, curRec, attrDesc1, attrDesc1) != 0) 
        	        break;
        	    
        	    relSubset1.push_back(rec1);
        	}
            
        // Advance rec2 until its == to rec1
       	 while (Operators::matchRec(curRec, rec2, attrDesc1, attrDesc2) > 0) {
          	  if (file2.next(rec2) != OK) 
			return OK;
          	  data2 = (char*)rec2.data;
         
           	 if (Operators::matchRec(curRec, rec2, attrDesc1, attrDesc2) < 0) {
               		relSubset1.clear();
                	relSubset2.clear();
                	test = true;
        
               		break;
            	}
        }
        
        if (test) {
        	test = false;
        	continue;
        }
            
      
        for(;;){
            relSubset2.push_back(rec2);
            
            if (file2.next(rec2) != OK)  
		break; 
            data2 = (char*)rec2.data;
            
            if (Operators::matchRec(rec2, curRec, attrDesc2, attrDesc1) != 0) 
            
                break;
            
        }
            
        // Set curData to new data
        curData = data1;
        curRec = rec1;
        
        // get the result
        for(vector<Record>::iterator recIt1=relSubset1.begin();recIt1!=relSubset1.end();++recIt1) {
            for(vector<Record>::iterator recIt2=relSubset2.begin();recIt2!=relSubset2.end();++recIt2) {
            
                Record record;
                record.data = malloc(size);
                record.length = size;
                
                // treat the result
                for (int i=0;i<projCnt;i++) {
                    AttrDesc outAttr = attrDescArray[i];
                    if (strcmp(outAttr.relName, attrDesc1.relName) == 0){
                        memcpy(((char*)record.data)+rel[i].attrOffset,
                                ((char*)(*recIt1).data)+outAttr.attrOffset,
                                outAttr.attrLen);
                    }
                    else {
                        memcpy(((char*)record.data)+rel[i].attrOffset,
                                ((char*)(*recIt2).data)+outAttr.attrOffset,
                                outAttr.attrLen);
                    }
                }
                RID rid;
                status = heapRes.insertRecord(record, rid);
                if (status != OK) 
			return status;
            }
        }
    }    
    return OK;
}


