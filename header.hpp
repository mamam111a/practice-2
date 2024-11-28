#pragma once
#include <iostream>
#include <string>
#include <algorithm>
#include <sstream>
#include <string>
#include "json.hpp"
#include <fstream>
#include <filesystem>
#include <sstream>
#include <locale>
using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;


struct Node {
    int numColumn;
    string name;
    string cell;
    Node* next;
};
struct RowNode {
    string name;
    Node* cell;
    RowNode* nextRow;
};
struct Condition {
    bool trueOrFalse;
    string condition;
    string oper;
    Condition* next;
};

RowNode* InsertInto(RowNode* table, const string listString[]);
RowNode* SelectFromOneTable(RowNode* table, int numColumns[]);
RowNode* SelectFromManyTables(RowNode** tables, int numColumns[], int countTables);
RowNode* SelectFromCartesian(RowNode** tables, int countTables);
Condition* SplitCondition(const string& filter);
bool CheckingCondition(RowNode* row, const string& condition);
bool CheckingLogicalExpression(RowNode* row, Condition* condition);
RowNode* FilteringTable(RowNode** select, RowNode** where, int selectSize, int whereSize, int numColumnsSelect[], int numColumnsWhere[], string condition);
RowNode* DeleteFrom(RowNode* table, Condition* condition);
void CreateTable(const string& schemaDir, const string& tableName, ostringstream& toClient);
RowNode* AddNumColumns(RowNode* table, int numColumns[], int countTables);
RowNode* AddTableNames(RowNode* table, RowNode** tables, int countTables);
void FreeTable(RowNode* table);
void FreeAllTables(RowNode** tables, int count);
void PrintTable(RowNode* table, ostringstream& toClient);
void PrintTables(RowNode** tables, ostringstream& toClient);
void Lock(const string& tableName, bool parameter, ostringstream& toClient);
void incrementSequence(const string& tableName, ostringstream& toClient);
void ReadConfiguration(const string& filename, ostringstream& toClient);
void UpdatePrimaryKey(const string& schemaName, const string& tableName, RowNode* table, ostringstream& toClient);
void WriteJsonToCSV(const string& filePath, const json& jsonData,ostringstream& toClient );
json ConvertTableToJson(RowNode* table);
RowNode* ConvertJsonToTable(const json& jsonData, const string& tableName, ostringstream& toClient);
json ReadCSVToJson(const string& filePath);
void WriteJsonToCSV(const string& filePath, const json& jsonData, ostringstream& toClient);
void RewriteCSVbyJson(const string& filePath, const json& jsonData, ostringstream& toClient);
void WriteUtf8BOM(ofstream& file);
int countRowsInCSV(const string& csvFilePath, ostringstream& toClient);
int getNextCsv(const string& tableName, ostringstream& toClient);
void AddColumnsInSchemaJson(const json& newColumns, const string& schemaFilePath, const string& tableName, ostringstream& toClient);
void RewriteTableSchema(const json& newSchema, const string& schemaFilePath, const string& tableName, ostringstream& toClient);
void splitString(string& str, string* tokens);

int DBMS_Queries(string& command, ostringstream& toClient);