#include <iostream>
#include <string>
#include <algorithm>
#include <sstream>
#include <string>
#include "json.hpp"
#include <fstream>
#include <filesystem>
#include "header.hpp"
#include <locale>

using namespace std;
using json = nlohmann::json;
namespace fs = filesystem;

RowNode* AddTableNames(RowNode* table, RowNode** tables, int countTables) {
    RowNode* currRow = table;
    while (currRow != nullptr) {
        Node* currCell = currRow->cell;
        int i = 0;
        while (currCell != nullptr && i < countTables) {
            if (countTables == 1) {
                currCell->name = tables[0]->name;
                currCell = currCell->next;
            }
            else {
                currCell->name = tables[i]->name;
                currCell = currCell->next;
                i++;
            }
        }
        currRow = currRow->nextRow;
    }
    return table;
}
RowNode* AddNumColumns(RowNode* table, int numColumns[], int countTables) {
    RowNode* currRow = table;
    while (currRow != nullptr) {
        Node* currCell = currRow->cell;
        int i = 0;
        while (currCell != nullptr) {
            currCell->numColumn = numColumns[i];
            currCell = currCell->next;
            i++;
        }
        currRow = currRow->nextRow;
    }
    return table;
}

void FreeTable(RowNode* table) {
    while (table != nullptr) {
        RowNode* tempRow = table;
        table = table->nextRow;

        Node* currNode = tempRow->cell;
        while (currNode != nullptr) {
            Node* tempNode = currNode;
            currNode = currNode->next;
            delete tempNode;
        }
        delete tempRow;
    }
}
void FreeAllTables(RowNode** tables, int count) {
    for (int i = 0; i < count; ++i) {
        FreeTable(tables[i]); 
    }
}

void PrintTable(RowNode* table, ostringstream& toClient) {
    int i = 0;
    while (table != nullptr) {
        Node* currNode = table->cell;
        while (currNode != nullptr) {
            toClient << "\t" << currNode->cell;
            currNode = currNode->next;
        }
        table = table->nextRow;
        i++;
        toClient << endl;
    }
}
void PrintTables(RowNode** tables, ostringstream& toClient) {
    int i = 0;
    while (tables[i] != nullptr) {
        toClient << i + 1 << ":\n";
        PrintTable(tables[i], toClient);
        i++;
    }
}

void splitString(string& str, string* tokens) {

    if (!str.empty() && str.front() == '{') {
        str.erase(0, 1);
    }
    if (!str.empty() && str.back() == '}') {
        str.erase(str.size() - 1);
    }

    int count = 0;
    stringstream ss(str);
    string token;

    while (getline(ss, token, ',') && count < 1000) {
        //убираем пробелы в начале и конце токена
        token.erase(0, token.find_first_not_of(" \n\r\t"));
        token.erase(token.find_last_not_of(" \n\r\t") + 1);
        tokens[count++] = token; 
    }
}