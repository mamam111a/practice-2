#include <iostream>
#include <string>
#include <algorithm>
#include <sstream>
#include "json.hpp"
#include <fstream>
#include <filesystem>
#include "header.hpp"
#include <locale>

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

int DBMS_Queries(string& command, ostringstream& toClient) {
    string scheme = "Схема 1";
    //string command;
    try { 

        if (command.find("CREATE TABLE") != string::npos) {
            auto pos = command.find("CREATE TABLE");
            string nameTable = command.substr(pos + strlen("CREATE TABLE "));
            CreateTable(scheme, nameTable, toClient);
        }
        else if (command.find("INSERT INTO") != string::npos) {
            RowNode* tempTable = nullptr;
            auto pos = command.find("INSERT INTO") + strlen("INSERT INTO ");
            auto endPos = command.find(' ', pos);

            string nameTable = (endPos == string::npos)
                ? command.substr(pos)
                : command.substr(pos, endPos - pos);
            Lock(nameTable, 1, toClient);
            auto valuesStart = endPos + 1;
            string valuesString = command.substr(valuesStart);
            string value[100];
            splitString(valuesString, value);
            int csv = getNextCsv(nameTable, toClient);

            string line;
            tempTable = InsertInto(tempTable, value);
            json tempJson2 = ConvertTableToJson(tempTable);
            WriteJsonToCSV(scheme + "/" + nameTable + "/" + to_string(csv) + ".csv", tempJson2,toClient);
            AddColumnsInSchemaJson(tempJson2, "schema.json", nameTable, toClient);
            incrementSequence(nameTable, toClient);
            FreeTable(tempTable);
            Lock(nameTable,0, toClient);
        }
        else if (command.find("DELETE FROM") != string::npos) {
            auto pos = command.find("DELETE FROM");  
            if (pos != string::npos) {
                pos = command.find("WHERE", pos);  
                if (pos != string::npos) {
                    pos += strlen("WHERE");  
                }
            }
            auto posPoint = command.find(".");
            string nameTable = (posPoint == string::npos)
                ? command.substr(pos)
                : command.substr(pos, posPoint - pos);
            nameTable.erase(remove(nameTable.begin(), nameTable.end(), ' '), nameTable.end());
            Lock(nameTable, 1, toClient);
            auto posGap = command.find(" =");
            posPoint = posPoint + strlen("колонка") + 1;
            string numColumnStr = (posGap == string::npos)
                ? command.substr(posPoint)
                : command.substr(posPoint, posGap - posPoint);

            string value = command.substr(posGap + 3);
            string criterion = command.substr(pos);
            Condition* condition = SplitCondition(criterion);
            ifstream file("schema.json");
            json tempJson;
            file >> tempJson;
            file.close();
            RowNode* newTable = ConvertJsonToTable(tempJson, nameTable, toClient);
            RowNode* tempTables[] = { newTable };
            newTable->name = nameTable;
            AddTableNames(newTable, tempTables, 1);
            int i = 0;
            Node* temp = newTable->cell;
            while (temp != nullptr) {
                temp = temp->next;
                i++;
            }
            int countColumns[i];
            AddNumColumns(newTable, countColumns, 1);
            newTable = DeleteFrom(newTable, condition);
            UpdatePrimaryKey("Схема 1", nameTable, newTable, toClient);
            json tempJson1 = ConvertTableToJson(newTable);
            RewriteTableSchema(tempJson1, "schema.json", nameTable, toClient);
            RewriteCSVbyJson(scheme + "/" + nameTable + "/" + "1.csv", tempJson1, toClient);
            toClient << endl << "Строки были успешно удалены!" << endl;
            FreeTable(newTable);
            Lock(nameTable,0, toClient);
        }

        else if (command.find("SELECT") != string::npos) {
            string namesTables;
            if (command.find("*") != string::npos) {
                int pos = command.find("SELECT * FROM ") + strlen("SELECT * FROM ");
                namesTables = command.substr(pos);

                stringstream ss(namesTables);
                string tableName;
                string tables[2];
                int count = 0;

                while (getline(ss, tableName, ',')) {
                    tableName.erase(remove(tableName.begin(), tableName.end(), ' '), tableName.end());
                    if (count < 2) {
                        tables[count] = tableName;
                        Lock(tableName, 1, toClient);
                        count++;
                    }
                }

                if (count < 2) {
                    toClient << "Недостаточное количество таблиц. Необходимо две." << endl;
                    return 0;
                }

                ifstream file("schema.json");
                json tempJson;
                file >> tempJson;
                file.close();
                RowNode* newTableA = ConvertJsonToTable(tempJson, tables[0], toClient);
                RowNode* newTableB = ConvertJsonToTable(tempJson, tables[1], toClient);
                RowNode* newTables[] = { newTableA, newTableB };
                RowNode* crossTable = SelectFromCartesian(newTables, 2);
                toClient << endl << "Пересечение двух таблиц: " << endl;
                PrintTable(crossTable, toClient);
                FreeTable(crossTable);
                Lock(tables[0], 0, toClient);
                Lock(tables[1], 0, toClient);
            }
            else if (command.find("FROM") != string::npos && command.find("WHERE") == string::npos) {
                int pos = command.find("SELECT") + strlen("SELECT ");
                namesTables = command.substr(pos);

                stringstream ss(namesTables);
                string tableName;
                string tables[5];
                int numColumns[5];
                int count = 0;

                while (getline(ss, tableName, ',')) {

                    tableName.erase(remove(tableName.begin(), tableName.end(), ' '), tableName.end());

                    size_t pos = tableName.find('.');
                    if (pos != string::npos) {

                        string table = tableName.substr(0, pos);
                        int lenStrColumn = tableName.find("колонка");
                        string columnStr = tableName.substr(lenStrColumn + strlen("колонка"));
                        int column = stoi(columnStr);
                        if (count < 5) {
                            tables[count] = table;
                            Lock(table,1, toClient);
                            numColumns[count] = column;
                            count++;
                        }
                    }
                    else {
                        toClient << "Некорректный формат: " << tableName << endl;
                        continue;
                    }
                }

                ifstream file("schema.json");
                json tempJson;
                file >> tempJson;

                if (count > 5) {
                    toClient << "Некорректное количество таблиц" << endl;
                    return 0;
                }

                RowNode* newTables[5];
                for (int i = 0; i < count; i++) {
                    RowNode* temp = ConvertJsonToTable(tempJson, tables[i], toClient);
                    temp->name = tables[i];
                    newTables[i] = temp;
                    Lock(tables[i], 0, toClient);

                }

                RowNode* crossTable = SelectFromManyTables(newTables, numColumns, count);
                toClient << endl << "Итоговая таблица: " << endl;
                PrintTable(crossTable, toClient);
                FreeTable(crossTable);
                file.close();
            }

            else if (command.find("FROM") != string::npos && command.find("WHERE") != string::npos) {
                int pos1 = command.find("SELECT") + strlen("SELECT ");
                int posFrom = command.find("FROM");
                string namesTablesSelect = command.substr(pos1, posFrom - pos1);

                stringstream ss1(namesTablesSelect);
                string tableNameSelect;
                string tablesSelect[2];
                int numColumnsSelect[2];
                int count1 = 0;

                while (getline(ss1, tableNameSelect, ',')) {

                    tableNameSelect.erase(remove(tableNameSelect.begin(), tableNameSelect.end(), ' '), tableNameSelect.end());
                    size_t pos = tableNameSelect.find('.');
                    if (pos != string::npos) {

                        string table = tableNameSelect.substr(0, pos);
                        int lenStrColumn = tableNameSelect.find("колонка");
                        string columnStr = tableNameSelect.substr(lenStrColumn + strlen("колонка"));
                        int column = stoi(columnStr);
                        if (count1 < 2) {
                            tablesSelect[count1] = table;
                            Lock(table, 1, toClient);
                            numColumnsSelect[count1] = column;
                            count1++;
                        }

                    }
                }
                if (count1 > 2) {
                    toClient << "Некорректное количество таблиц" << endl;
                    return 0;
                }

                ifstream file("schema.json");
                json tempJson;
                file >> tempJson;
                file.close();


                RowNode* newTablesSelect[2];
                for (int i = 0; i < count1; i++) {
                    RowNode* temp = ConvertJsonToTable(tempJson, tablesSelect[i], toClient);
                    temp->name = tablesSelect[i];
                    Lock(tablesSelect[i], 0, toClient);
                    newTablesSelect[i] = temp;
                }
            
                int posWhere = command.find("WHERE") + strlen("WHERE ");
                string namesTablesWhere;
                Condition* current = SplitCondition(command.substr(posWhere));

                while (current != nullptr) {
                    if (!namesTablesWhere.empty()) {
                        namesTablesWhere += ", ";
                    }
                    int posEqual = current->condition.find('=');
                    if (posEqual != string::npos) {
                        string leftSide = current->condition.substr(0, posEqual);
                        namesTablesWhere += leftSide;
                    }
                    if(current->condition.find("колонка")!= string::npos) {
                        string rightSide= current->condition.substr(posEqual + 1);
                        namesTablesWhere += ", " + rightSide;
                    }

                    current = current->next;
                }
                stringstream ss2(namesTablesWhere);
                string tableNameWhere;
                string tablesWhere[2];
                int numColumnsWhere[2];
                int count2 = 0;

                while (getline(ss2, tableNameWhere, ',')) {

                    tableNameWhere.erase(remove(tableNameWhere.begin(), tableNameWhere.end(), ' '), tableNameWhere.end());

                    size_t pos = tableNameWhere.find('.');
                    if (pos != string::npos) {

                        string table = tableNameWhere.substr(0, pos);
                        int lenStrColumn = tableNameWhere.find("колонка");
                        string columnStr = tableNameWhere.substr(lenStrColumn + strlen("колонка"));
                        int column = stoi(columnStr);
                        if (count2 < 2) {
                            tablesWhere[count2] = table;
                            Lock(table, 1, toClient);
                            numColumnsWhere[count2] = column;
                            count2++;
                        }
                    }
                }
                ifstream file1("schema.json");
                json tempJson1;
                file1 >> tempJson1;
                file1.close();

                RowNode* newTablesWhere[2];
                for (int i = 0; i < count2; i++) {
                    RowNode* temp = ConvertJsonToTable(tempJson, tablesWhere[i], toClient);
                    temp->name = tablesWhere[i];
                    Lock(tablesWhere[i], 0, toClient);
                    newTablesWhere[i] = temp;
                }
                string filter = command.substr(posWhere);
                RowNode* crossTable = FilteringTable(newTablesSelect, newTablesWhere, count1, count2, numColumnsSelect, numColumnsWhere, filter);
                toClient << endl << "Итоговая таблица: " << endl;
                PrintTable(crossTable, toClient);

                FreeAllTables(newTablesSelect, count1);
                FreeAllTables(newTablesWhere, count2);
                file.close();

            }
        }
        else {
            toClient << endl << "Некорректный ввод" << endl;
        }
    
    }
    catch(...) {
        toClient << endl << "Неизвестная ошибка!" << endl;
    }
    return 0;
}

