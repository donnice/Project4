#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
	Status status;
	Status status2;
	status2 = OK;
	if(status2 != OK)
		return status2;

	AttrDesc name[projCnt];
	for(int i = 0; i < projCnt; i++)
	{
		AttrDesc retAttr;
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, retAttr);
		if(status != OK) 
			return status;

		name[i] = retAttr;
	}

	int recLength = 0;
	for(int i = 0; i < projCnt; i++)
	{
		recLength += projNames[i].attrLen;
	}	

	// if an attr exists
	if(attr)
	{
		AttrDesc attrDesc;

		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if(status != OK) 
			return status;

		//If there is index which is indexed
		if(op == 2 && attrDesc.indexed)
		{
			status = Operators::IndexSelect(result, projCnt, name, &attrDesc, op, attrValue, recLength);
			if(status != OK) 
				return status;
		}
		else
		{
			status = Operators::ScanSelect(result, projCnt, name, &attrDesc, op, attrValue, recLength);
			if(status != OK) 
				return status;
		}
	}
	else
	{
		status = Operators::ScanSelect(result, projCnt, name, NULL, op, attrValue, recLength);
	}
	if(status != OK)
		return status;

	return OK;
}
