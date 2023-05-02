#include "catalog.h"
#include "query.h"
#include "string.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{

AttrDesc *descriptions; 
Status status;
int true_attrc;

status = attrCat->getRelInfo(relation, true_attrc, descriptions);
if (status != OK) return status;
if (attrCnt != true_attrc) return UNIXERR;

int tot_record_len = 0;
for (int i = 0; i < true_attrc; i++) {
	tot_record_len += descriptions[i].attrLen;
}

char* payload = new char[tot_record_len];

for (int i = 0; i < true_attrc; i++) {
	bool end_list = true;
	for (int j = 0; j < true_attrc; j++) {
		if (strcmp(attrList[i].attrName, descriptions[j].attrName) == 0) {
			if (descriptions[j].attrType == FLOAT) {
				float val = atof((char*) attrList[i].attrValue);
				memcpy(payload + descriptions[j].attrOffset, &val, descriptions[j].attrLen);
			}
			else if (descriptions[j].attrType == INTEGER) {
				int val = atoi((char*) attrList[i].attrValue);
				memcpy(payload + descriptions[j].attrOffset, &val, descriptions[j].attrLen);
			}
			else if (descriptions[j].attrType == STRING) {
				memcpy(payload + descriptions[j].attrOffset, attrList[i].attrValue, descriptions[j].attrLen);
			}
			end_list = false;
			break;
		}
	}
	if (end_list) {
		free(descriptions);
		delete payload;
		return UNIXERR;
	}
}
InsertFileScan newScan(relation, status);

Record toInsert;
toInsert.data = (void*) payload;
toInsert.length = tot_record_len;

RID temp;
status = newScan.insertRecord(toInsert, temp);
if (status != OK) return status;
free(descriptions);
delete payload;

return OK;
}