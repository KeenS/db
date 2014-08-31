/*
 * データ定義モジュールテストプログラム
 */

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "microdb.h"

/*
 * main -- データ定義モジュールのテスト
 */
int main(int argc, char **argv)
{
    char tableName[20];
    TableInfo tableInfo;
    int i;

    /* ファイルモジュールを初期化する */
    if (initializeFileModule() != OK) {
	fprintf(stderr, "Cannot initialize file module.\n");
	exit(1);
    }

    /* データ定義ジュールを初期化する */
    if (initializeDataDefModule() != OK) {
	fprintf(stderr, "Cannot initialize data definition module.\n");
	exit(1);
    }

    /*
     * このプログラムの前回の実行の時のデータ定義残っている可能性があるので、
     * とりあえず削除する
     */
    dropTable("student");
    dropTable("teacher");

    /*
     * 以下の入力を想定してテスト
     * create table student (
     *   id string,
     *   name string,
     *   age integer,
     *   address string
     * )
     */
    strcpy(tableName, "student");
    i = 0;

    strcpy(tableInfo.fieldInfo[i].name, "id");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "name");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "age");
    tableInfo.fieldInfo[i].dataType = TYPE_INTEGER;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "address");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    tableInfo.numField = i;

    /* テーブルの作成 */
    if (createTable(tableName, &tableInfo) != OK) {
	/* テーブルの作成に失敗 */
	fprintf(stderr, "Cannot create table.\n");
	exit(1);
    }

    /* 作成したテーブルの情報を出力 */
    printTableInfo(tableName);

    /*
     * 以下の入力を想定してテスト
     * create table teacher (
     *   id string,
     *   name string,
     *   title string,
     *   age integer,
     *   address string,
     *   class integer
     * )
     */
    strcpy(tableName, "teacher");
    i = 0;

    strcpy(tableInfo.fieldInfo[i].name, "id");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "name");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "title");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "age");
    tableInfo.fieldInfo[i].dataType = TYPE_INTEGER;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "address");
    tableInfo.fieldInfo[i].dataType = TYPE_STRING;
    i++;

    strcpy(tableInfo.fieldInfo[i].name, "class");
    tableInfo.fieldInfo[i].dataType = TYPE_INTEGER;
    i++;

    tableInfo.numField = i;

    /* テーブルの作成 */
    if (createTable(tableName, &tableInfo) != OK) {
	/* テーブルの作成に失敗 */
	fprintf(stderr, "Cannot create table.\n");
	exit(1);
    }

    /* 作成したテーブルの情報を出力 */
    printTableInfo(tableName);

    /* 後始末 */
    /* dropTable("student"); */
    /* dropTable("teacher"); */
    finalizeDataDefModule();
    finalizeFileModule();
}
