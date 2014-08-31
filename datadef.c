#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "microdb.h"
/* #include <sys/types.h> */
/* #include <sys/stat.h> */
/* #include <fcntl.h> */

#define DEF_FILE_EXT ".def"

Result
initializeDataDefModule()
{
  return OK;
}

Result
finalizeDataDefModule()
{
  return OK;
}

char *
filenameOf(char *tableName)
{
  int len = MAX_FILENAME + strlen(DEF_FILE_EXT) + 1;
  char *buf = malloc(sizeof(char) * len);
  strcpy(buf, tableName);
  strcat(buf, DEF_FILE_EXT);
  return buf;
}

Result
createDefFile(char *tableName){
  char* filename = filenameOf(tableName);
  Result result;
  result = createFile(filename);
  free(filename);
  return result;
}


Result
createTable(char *tableName, TableInfo *tableInfo)
{
  char* filename = filenameOf(tableName);

  if(createDataFile(tableName) == NG){
    return NG;
  }
  if(createDefFile(tableName) == NG){
    return NG;
  }
  File *file = openFile(filename);
  if(file->fd == -1){
    return NG;
  }
  char page[PAGE_SIZE];
  char *p;int page_num = 0;
  memset(page, 0, PAGE_SIZE);
  p = page;

#define APPEND_PAGE(data)                                               \
  if((unsigned long) (page+PAGE_SIZE) < ((unsigned long) p) + sizeof(data)){ \
    if(writePage(file, page_num++, page) == NG){                        \
      return NG;                                                        \
    }                                                                   \
    memset(page, 0, PAGE_SIZE);                                         \
    p = page;                                                           \
  }                                                                     \
  memcpy(p , &(data), sizeof(data));                                    \
  p = (char *) (((unsigned long) p) + sizeof(data));                    \


  APPEND_PAGE(tableInfo->numField);

  FieldInfo *f;
  for(int i = 0; i < tableInfo->numField; i++){
    f = &tableInfo->fieldInfo[i];
    APPEND_PAGE(f->name);
    APPEND_PAGE(f->dataType);
  }
  if(writePage(file, 0, page) == NG){
    return NG;
  };
  closeFile(file);
  free(filename);
  return OK;
}

static Result
deleteDefFile(char *tableName){
  char* filename = filenameOf(tableName);
  Result result;
  result = deleteFile(filename);
  free(filename);
  return result;  
}

Result
dropTable(char *tableName)
{
  Result result;
  result = deleteDefFile(tableName);
  result = deleteDataFile(tableName);
  return result;
}



TableInfo
*getTableInfo(char *tableName)
{
  char *filename = filenameOf(tableName);
  File *file = openFile(filename);
  if(file->fd == -1){
    return NULL;
  }
  TableInfo *t_info =  malloc(sizeof(TableInfo));

  char page[PAGE_SIZE];
  char *p; int page_num = 0;
  p = page;
  if (readPage(file, page_num, page) == NG){
    return NULL;
  };

#define READ_DATA(dist)                                                \
  if((unsigned long )(page+PAGE_SIZE) < ((unsigned long)p) + sizeof(dist)){ \
    if(readPage(file,++page_num, page) == NG){                          \
      return NULL;                                                      \
    }                                                                   \
    p = page;                                                           \
  }                                                                     \
  memcpy(&(dist), p, sizeof(dist));                                     \
  p = (char *)(((unsigned long)p) + sizeof(dist));

  READ_DATA(t_info->numField);

  for(int i = 0; i < t_info->numField; i++){
    READ_DATA(t_info->fieldInfo[i].name);
    READ_DATA(t_info->fieldInfo[i].dataType);
  }
  return t_info;
}

void
freeTableInfo(TableInfo *t_info){
  free(t_info);
  return;
}
void printTableInfo(char *tableName)
{
    TableInfo *tableInfo;
    int i;

    /* テーブル名を出力 */
    printf("\nTable %s\n", tableName);

    /* テーブルの定義情報を取得する */
    if ((tableInfo = getTableInfo(tableName)) == NULL) {
	/* テーブル情報の取得に失敗したので、処理をやめて返る */
	return;
    }

    /* フィールド数を出力 */
    printf("number of fields = %d\n", tableInfo->numField);

    /* フィールド情報を読み取って出力 */
    for (i = 0; i < tableInfo->numField; i++) {
	/* フィールド名の出力 */
	printf("  field %d: name = %s, ", i + 1, tableInfo->fieldInfo[i].name);

	/* データ型の出力 */
	printf("data type = ");
	switch (tableInfo->fieldInfo[i].dataType) {
	case TYPE_INTEGER:
	    printf("integer\n");
	    break;
	case TYPE_STRING:
	    printf("string\n");
	    break;
	default:
	    printf("unknown\n");
	}
    }

    /* データ定義情報を解放する */
    freeTableInfo(tableInfo);

    return;
}

