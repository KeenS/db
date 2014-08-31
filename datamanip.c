#include <stdlib.h>
#include <string.h>
#include "microdb.h"
#define DATA_FILE_EXT ".dat"

static char*
fileNameOf(char* name){
  char *result;
  size_t len;
  len = strlen(name);
  result = malloc(len + sizeof(DATA_FILE_EXT));
  memcpy(result, name, len);
  strcat(result, DATA_FILE_EXT);
  return result;
}

static Result
checkCondition(Condition *, RecordData *);

Result initializeDataManipModule()
{
  return OK;
}

Result finalizeDataManipModule()
{
  return OK;
}

/*
 * getRecordSize -- 1レコード分の保存に必要なバイト数の計算
 *
 * 引数:
 *	tableInfo: データ定義情報を収めた構造体
 *
 * 返り値:
 *	tableInfoのテーブルに収められた1つのレコードを保存するのに
 *	必要なバイト数
 */
static int getRecordSize(TableInfo *tableInfo)
{
  int res = 0;
  for (int i = 0; i < tableInfo->numField; i++) {
    switch (tableInfo->fieldInfo[i].dataType) {
    case TYPE_INTEGER:
      res += sizeof(int);
      break;
    case TYPE_STRING:
      res += MAX_STRING;
      break;
    case TYPE_UNKNOWN:
      return -1;
    }
  }
  return res;
}

Result
insertRecord(char *tableName, RecordData *recordData)
{
  TableInfo *tableInfo;
  char *fileName;
  File *file;
  int recordSize;
  int numPage;
  char page[PAGE_SIZE];
  char *record;
  char *p;

  /* テーブルの情報を取得する */
  if ((tableInfo = getTableInfo(tableName)) == NULL) {
    return NG;
  }

  recordSize = getRecordSize(tableInfo);

  if ((p = record = malloc(recordSize)) == NULL) {
    return NG;
  }

  /* 先頭に、「使用中」を意味するフラグを立てる */
  memset(p, 1, 1);
  p += 1;

  /* 確保したメモリ領域に、フィールド数分だけ、順次データを埋め込む */
  for (int i = 0; i < tableInfo->numField; i++) {
    switch (tableInfo->fieldInfo[i].dataType) {
    case TYPE_INTEGER:
      memset(p, recordData->fieldData[i].intValue, sizeof(recordData->fieldData[i].intValue));
      p = (char *)((unsigned long)p + sizeof(recordData->fieldData[i].intValue));
      break;
    case TYPE_STRING:
      memcpy(p, recordData->fieldData[i].stringValue, sizeof(recordData->fieldData[i].stringValue));
      p = (char *)((unsigned long)p + sizeof(recordData->fieldData[i].stringValue));
      break;
    case TYPE_UNKNOWN:
      /* ここにくることはないはず */
      freeTableInfo(tableInfo);
      free(record);
      return NG;
    }
  }
  freeTableInfo(tableInfo);


  fileName = fileNameOf(tableName);

  file = openFile(fileName);
  numPage = getNumPages(fileName);

  /* レコードを挿入できる場所を探す */
  int i;
  for (i = 0; i < numPage; i++) {
    /* 1ページ分のデータを読み込む */
    if (readPage(file, i, page) != OK) {
      free(record);
      return NG;
    }
    /* pageの先頭からrecordSizeバイトずつ飛びながら、先頭のフラグが「0」(未使用)の場所を探す */
    for (int j = 0; j < (PAGE_SIZE / recordSize); j++) {
      char *q;
      q = page + j*recordSize;
      if (*q == 0) {
        /* 見つけた空き領域に上で用意したバイト列recordを埋め込む */
        memcpy(q, record, recordSize);

        /* ファイルに書き戻す */
        if (writePage(file, i, page) != OK) {
          free(record);
          return NG;
        }
        closeFile(file);
        free(record);
        return OK;
      }
    }
  }

  /*
   * ファイルの最後まで探しても未使用の場所が見つからなかったら
   * ファイルの最後に新しく空のページを用意し、そこに書き込む
   */
  memset(page, 0, PAGE_SIZE);
  memcpy(page, record, recordSize);
  if (writePage(file, i, page) != OK) {
    free(record);
    return NG;
  }
  closeFile(file);
  free(record);
  return OK;
}

Result
deleteRecord(char *tableName, Condition *condition)
{
  char page[PAGE_SIZE];
  char *fileName = fileNameOf(tableName);
  File *file = openFile(fileName);
  int numPage = getNumPages(fileName);
  TableInfo *tableInfo;
  RecordData *recordData;
  /* テーブルの情報を取得する */
  if ((tableInfo = getTableInfo(tableName)) == NULL) {
    return NG;
  }

  int recordSize = getRecordSize(tableInfo);
  recordData = malloc(sizeof(RecordData));
  recordData->numField = tableInfo->numField;
  for (int i = 0; i < numPage; i++) {
    /* 1ページ分のデータを読み込む */
    if (readPage(file, i, page) != OK) {
      return NG;
    }

    for (int j = 0; j < (PAGE_SIZE / recordSize); j++) {
      char *p;
      p = page + j*recordSize;
      if (*p == 0) {
        continue;
      }
      p++;

      for (int k = 0; k < (recordData->numField = tableInfo->numField); k++) {
        FieldData *fdata = &(recordData->fieldData[k]);
        FieldInfo *finfo = &(tableInfo->fieldInfo[k]);
        memcpy(&(fdata->name), &(finfo->name), sizeof(finfo->name));
        switch ((fdata->dataType = finfo->dataType)) {
        case TYPE_INTEGER:
          memcpy(&(fdata->intValue), p, sizeof(fdata->intValue));
          p = (char *)((unsigned long)p + sizeof(fdata->intValue));
          break;
        case TYPE_STRING:
          memcpy(&(fdata->stringValue), p, sizeof(fdata->stringValue));
          p = (char *)((unsigned long)p + sizeof(fdata->stringValue));
          break;
        case TYPE_UNKNOWN:
          /* ここにくることはないはず */
          freeTableInfo(tableInfo);
          free(recordData);
          return NG;
        }
      }
      if(checkCondition(condition, recordData) == OK){
        memset(page + j*recordSize, 0, 1);
      }
    }
    if (writePage(file, i, page) != OK) {
      closeFile(file);
      free(recordData);
      return NG;
    }
  }
  closeFile(file);
  free(recordData);
  return OK;
}

/*
 * selectRecord -- レコードの検索
 *
 * 引数:
 *	tableName: レコードを検索するテーブルの名前
 *	condition: 検索するレコードの条件
 *
 * 返り値:
 *	検索に成功したら検索されたレコード(の集合)へのポインタを返し、
 *	検索に失敗したらNULLを返す。
 *	検索した結果、該当するレコードが1つもなかった場合も、レコードの
 *	集合へのポインタを返す(中に長さ0のリストが入っている)。
 *
 * ***注意***
 *	この関数が返すレコードの集合を収めたメモリ領域は、不要になったら
 *	必ずfreeRecordSetで解放すること。
 */
RecordSet
*selectRecord(char *tableName, Condition *condition)
{
  char page[PAGE_SIZE];
  char *fileName = fileNameOf(tableName);
  File *file = openFile(fileName);
  int numPage = getNumPages(fileName);
  TableInfo *tableInfo;
  RecordData *recordData, *tail;
  RecordSet *recordSet;
  /* テーブルの情報を取得する */
  if ((tableInfo = getTableInfo(tableName)) == NULL) {
    return NULL;
  }

  int recordSize = getRecordSize(tableInfo);
  tail = malloc(sizeof(RecordData));
  recordSet = malloc(sizeof(RecordSet));
  recordSet->numRecord = 0;
  recordData = malloc(sizeof(RecordData));
  recordData->numField = tableInfo->numField;
  for (int i = 0; i < numPage; i++) {
    /* 1ページ分のデータを読み込む */
    if (readPage(file, i, page) != OK) {
      return NULL;
    }

    for (int j = 0; j < (PAGE_SIZE / recordSize); j++) {
      char *p;
      p = page + j*recordSize;
      if (*p == 0) {
        continue;
      }
      p++;
      if(recordData == NULL){
        recordData = malloc(sizeof(RecordData));
      }
      for (int k = 0; k < (recordData->numField = tableInfo->numField); k++) {
        FieldData *fdata = &(recordData->fieldData[k]);
        FieldInfo *finfo = &(tableInfo->fieldInfo[k]);
        memcpy(&(fdata->name), &(finfo->name), sizeof(finfo->name));
        switch ((fdata->dataType = finfo->dataType)) {
        case TYPE_INTEGER:
          fdata->intValue = (int) *p;
          /* memcpy(&(fdata->intValue), p, sizeof(fdata->intValue)); */
          p = (char *)((unsigned long)p + sizeof(fdata->intValue));
          break;
        case TYPE_STRING:
          memcpy(&(fdata->stringValue), p, sizeof(fdata->stringValue));
          p = (char *)((unsigned long)p + sizeof(fdata->stringValue));
          break;
        case TYPE_UNKNOWN:
          /* ここにくることはないはず */
          freeTableInfo(tableInfo);
          free(recordData);
          return NULL;
        }
      }
      if(checkCondition(condition, recordData) == OK){
        tail->next = recordData;
        recordData = NULL;
        recordSet->numRecord++;
      }
    }
  }
  if(recordData != NULL){
    free(recordData);
  }
  recordSet->recordData = tail->next;
  return recordSet;
}

void
freeRecordSet(RecordSet *recordSet)
{
  RecordData *record, *next;
  next = recordSet->recordData;
  while(next){
    record = next;
    next = next->next;
    free(record);
  }
  free(recordSet);
}

static Result
checkCondition(Condition *condition, RecordData *recordData)
{
  for (int i = 0; i < recordData->numField ; i++) {
    /* フィールド名が同じかどうかチェック */
    if (strcmp(recordData->fieldData[i].name, condition->name) == 0) {
      if(recordData->fieldData[i].dataType != condition->dataType)
        return NG;
      switch(recordData->fieldData[i].dataType){
      case TYPE_INTEGER:{
        int n = recordData->fieldData[i].intValue;
        int m = condition->intValue;
        
#define GEN_INT_CASE(name, op)  case OP_##name:  return n op m ? OK : NG;
        switch(condition->operator){
          GEN_INT_CASE(EQUAL, ==)
            GEN_INT_CASE(NOT_EQUAL, !=)
            GEN_INT_CASE(GREATER_THAN, >)
            GEN_INT_CASE(LESS_THAN, <)
            }
        break;
      }
      case TYPE_STRING:{
        char *str1 = recordData->fieldData[i].stringValue;
        char *str2 = condition->stringValue;
        
#define GEN_STR_CASE(name, op)  case OP_##name:  return strcmp(str1, str2) op 0 ? OK : NG;
        switch(condition->operator){
          GEN_STR_CASE(EQUAL, ==)
            GEN_STR_CASE(NOT_EQUAL, !=)
            GEN_STR_CASE(GREATER_THAN, >)
            GEN_STR_CASE(LESS_THAN, <)
            }
        break;
      }
      case TYPE_UNKNOWN:
        return NG;
      }
    }
  }  
}
Result
createDataFile(char *tableName)
{
  char *name = fileNameOf(tableName);
  Result result;
  result = createFile(name);
  free(name);
  return result;
}

Result
deleteDataFile(char *tableName)
{
  char *name = fileNameOf(tableName);
  Result result;
  result = deleteFile(name);
  free(name);
  return result;
}
/*
 * printTableData -- すべてのデータの表示(テスト用)
 *
 * 引数:
 *	tableName: データを表示するテーブルの名前
 */
void printTableData(char *tableName)
{
  TableInfo *tableInfo;
  File *file;
  int i, j, k;
  int recordSize;
  int numPage;
  char *filename;
  char page[PAGE_SIZE];

  /* テーブルのデータ定義情報を取得する */
  if ((tableInfo = getTableInfo(tableName)) == NULL) {
    return;
  }

  /* 1レコード分のデータをファイルに収めるのに必要なバイト数を計算する */
  recordSize = getRecordSize(tableInfo);

  filename = fileNameOf(tableName);
  /* データファイルのページ数を求める */
  numPage = getNumPages(filename);

  /* データファイルをオープンする */
  if ((file = openFile(filename)) == NULL) {
    free(filename);
    freeTableInfo(tableInfo);
    return;
  }

  free(filename);

  /* レコードを1つずつ取りだし、表示する */
  for (i = 0; i < numPage; i++) {
    /* 1ページ分のデータを読み込む */
    readPage(file, i, page);

    /* pageの先頭からrecord_sizeバイトずつ切り取って処理する */
    for (j = 0; j < (PAGE_SIZE / recordSize); j++) {
      /* 先頭の「使用中」のフラグが0だったら読み飛ばす */
      char *p = page + recordSize * j;
      if (*p == 0) {
        continue;
      }

      /* フラグの分だけポインタを進める */
      p++;

      /* 1レコード分のデータを出力する */
      for (k = 0; k < tableInfo->numField; k++) {
        int intValue;
        char stringValue[MAX_STRING];

        printf("Field %s = ", tableInfo->fieldInfo[k].name);

        switch (tableInfo->fieldInfo[k].dataType) {
        case TYPE_INTEGER:
          intValue = (int) *p;
          /* memcpy(&intValue, p, sizeof(int)); */
          p = (char *)((unsigned long) p + sizeof(int));
          printf("%d\n", intValue);
          break;
        case TYPE_STRING:
          memcpy(stringValue, p, MAX_STRING);
          p += MAX_STRING;
          printf("%s\n", stringValue);
          break;
        case TYPE_UNKNOWN:
          /* ここに来ることはないはず */
          return;
        }
      }

      printf("\n");
    }
  }
}

/*
 * printRecordSet -- レコード集合の表示
 *
 * 引数:
 *	recordSet: 表示するレコード集合
 */
void printRecordSet(RecordSet *recordSet)
{
  RecordData *record;
  int i;

  /* レコード数の表示 */
  printf("Number of Records: %d\n", recordSet->numRecord);

  /* レコードを1つずつ取りだし、表示する */
  for (record = recordSet->recordData; record != NULL; record = record->next) {
    /* すべてのフィールドのフィールド名とフィールド値を表示する */
    for (i = 0; i < record->numField; i++) {
      printf("Field %s = ", record->fieldData[i].name);

      switch (record->fieldData[i].dataType) {
      case TYPE_INTEGER:
        printf("%d\n", record->fieldData[i].intValue);
        break;
      case TYPE_STRING:
        printf("%s\n", record->fieldData[i].stringValue);
        break;
      case TYPE_UNKNOWN:
        /* ここに来ることはないはず */
        return;
      }
    }

    printf("\n");
  }
}
  
