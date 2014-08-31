#include <stdio.h>
#define ERROR(msg) fprintf(stderr,"%s\n", (msg));

#define MAX_FILENAME 256
#define PAGE_SIZE 4096

typedef enum {OK, NG} Result;
typedef struct File File;
struct File {
  int fd;
  char name[MAX_FILENAME];
};

extern Result initializeFileModule();
extern Result finalizeFileModule();
extern Result createFile(char *);
extern File *openFile(char *);
extern Result deleteFile(char *);
extern Result closeFile(File *);
extern Result readPage(File *, int, char *);
extern Result writePage(File *, int, char *);
extern int getNumPages(char *);


#define MAX_FIELD 40
#define MAX_FIELD_NAME 20
typedef enum DataType DataType;
enum DataType {
  TYPE_UNKNOWN = 0,
  TYPE_INTEGER = 1,
  TYPE_STRING = 2
};
typedef struct FieldInfo FieldInfo;
struct FieldInfo{
  char name[MAX_FIELD_NAME];
  DataType dataType;
};
typedef struct TableInfo TableInfo;
struct TableInfo {
  int numField;
  FieldInfo fieldInfo[MAX_FIELD];
};

extern Result initializeDataDefModule();
extern Result finalizeDataDefModule();
extern Result createTable(char *, TableInfo *);
extern Result dropTable(char *);
extern TableInfo *getTableInfo(char*);
extern void freeTableInfo(TableInfo *);
extern void printTableInfo(char *);

#define MAX_STRING 20

typedef struct FieldData FieldData;
struct FieldData{
  char name[MAX_FIELD_NAME];
  DataType dataType;
  int intValue;
  char stringValue[MAX_STRING];
};

typedef struct RecordData RecordData;
struct RecordData{
  int numField;
  FieldData fieldData[MAX_FIELD];
  RecordData *next;
};

typedef struct RecordSet RecordSet;
struct RecordSet{
  int numRecord;
  RecordData *recordData;
};

typedef enum OperatorType OperatorType;
enum OperatorType {
    OP_EQUAL,				/* = */
    OP_NOT_EQUAL,			/* != */
    OP_GREATER_THAN,			/* > */
    OP_LESS_THAN			/* < */
};

typedef struct Condition Condition;
struct Condition {
    char name[MAX_FIELD_NAME];		/* フィールド名 */
    DataType dataType;			/* フィールドのデータ型 */
    OperatorType operator;		/* 比較演算子 */
    int intValue;			/* integer型の場合の値 */
    char stringValue[MAX_STRING];	/* string型の場合の値 */
};

extern Result initializeDataManipModule();
extern Result finalizeDataManipModule();
extern Result insertRecord(char *tableName, RecordData *recordData);
extern Result deleteRecord(char *tableName, Condition *condition);
extern RecordSet *selectRecord(char *tableName, Condition *condition);
extern void freeRecordSet(RecordSet *recordSet);
extern Result createDataFile(char *tableName);
extern Result deleteDataFile(char *tableName);
extern void printRecordSet(RecordSet *recordSet);
extern void printTableData(char *);
