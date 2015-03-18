#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>
#include <stdio.h>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
	AttrDesc attr1Desc;
	AttrDesc attr2Desc;	
	
	Status res;
	Status status1;
	
	status1 = OK;
	if(status1 != OK) 
		return status1;

	res = attrCat->getInfo(attr1->relName, attr1->attrName, attr1Desc); 
	if(res != OK) 
		return res;

	res = attrCat->getInfo(attr2->relName, attr2->attrName, attr2Desc); 
	if(res != OK) 
		return res;	
	
	AttrDesc descProjNames[projCnt];
	
	// Calc output record len
	int size=0;
	int i = 0;
	while(i<projCnt) {
		size += projNames[i].attrLen;
		res = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, descProjNames[i]); 
		if(res != OK) 
			return res;
		i++;
	}

	//Order or precedence: INL, SMJ, SNL
	if(op == EQ && (attr1Desc.indexed)) // If at least one is indexed and EQ op
	{ 
		if(attr2Desc.indexed) 
		{
			//attr2 is indexed so must be right side
			res = Operators::INL(result, projCnt, descProjNames, attr1Desc, op, attr2Desc, size);
			if(res != OK) 
				return res;
		}
		else 
		{
			//attr1 is indexed so must be right side
			res = Operators::INL(result, projCnt, descProjNames, attr2Desc, op, attr1Desc, size);
			if(res != OK) 
				return res;
		}
	}
	else if( op == EQ && (attr2Desc.indexed))
	{
		if(attr2Desc.indexed) 
		{
			//attr2 is indexed so must be right side
			res = Operators::INL(result, projCnt, descProjNames, attr1Desc, op, attr2Desc, size);
			if(res != OK) 
				return res;
		}
		else 
		{
			//attr1 is indexed so must be right side
			res = Operators::INL(result, projCnt, descProjNames, attr2Desc, op, attr1Desc, size);
			if(res != OK) 
				return res;
		}
	}

	else if(op == EQ) // If neither is indexed, but is EQ op
	{
		res = Operators::SMJ(result, projCnt, descProjNames, attr2Desc, op, attr1Desc, size);
		if(res != OK) 
			return res;
	}
	else 
	{
		res = Operators::SNL(result, projCnt, descProjNames, attr1Desc, op, attr2Desc, size);
		if(res != OK) 
			return res;
	}
	return OK;
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : floatDiff;

        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}
