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

RowNode* InsertInto(RowNode* table, const string listString[]) {
    RowNode* newRow = new RowNode;
    newRow->cell = nullptr;
    newRow->nextRow = nullptr;

    Node* currNode = nullptr;
    for (int i = 0; !listString[i].empty(); i++) {
        Node* newNode = new Node;
        newNode->cell = listString[i];
        newNode->next = nullptr;

        if (newRow->cell == nullptr) {
            newRow->cell = newNode;
        }
        else {
            currNode->next = newNode;
        }
        currNode = newNode;
    }
    if (table == nullptr) {
        return newRow;
    }
    else {
        RowNode* currRow = table;
        while (currRow->nextRow != nullptr) {
            currRow = currRow->nextRow;
        }
        currRow->nextRow = newRow;
        return table;
    }
}
RowNode* SelectFromOneTable(RowNode* table, int numColumns[]) {
    RowNode* currRow = table;
    RowNode* lastRow = nullptr;
    RowNode* crossTable = nullptr;
    int sizeList = sizeof(numColumns) / sizeof(numColumns[0]);

    while (currRow != nullptr) {
        RowNode* newRow = new RowNode;
        newRow->cell = nullptr;
        newRow->nextRow = nullptr;

        Node* currCell = currRow->cell;
        int i = 1;
        Node* lastCell = nullptr;

        while (currCell != nullptr) {
            for (int num = 0; num < sizeList; num++) {
                if (numColumns[num] == i) {

                    Node* newCell = new Node;
                    newCell->cell = currCell->cell;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    }
                    else {
                        lastCell->next = newCell;
                    }
                    lastCell = newCell;

                }
            }
            currCell = currCell->next;
            i++;
        }

        if (crossTable == nullptr) {
            crossTable = newRow;
        }
        else {
            lastRow->nextRow = newRow;
        }

        lastRow = newRow;
        currRow = currRow->nextRow;
    }

    RowNode* temp[] = { table };
    crossTable = AddTableNames(crossTable, temp, 1);
    crossTable = AddNumColumns(crossTable, numColumns, 1);

    return crossTable;
}

RowNode* SelectFromManyTables(RowNode** tables, int numColumns[], int countTables) {

    int maxRows = 0;
    for (int i = 0; i < countTables; i++) {
        int rows = 0;
        RowNode* current = tables[i];
        while (current != nullptr) {
            rows++;
            current = current->nextRow;
        }
        if (rows > maxRows) {
            maxRows = rows;
        }
    }
    RowNode* crossTable = nullptr;
    RowNode* lastRow = nullptr;

    for (int i = 0; i < maxRows; i++) {

        RowNode* newRow = new RowNode;
        newRow->cell = nullptr;
        newRow->name = "";
        newRow->nextRow = nullptr;

        Node* lastCell = nullptr;

        for (int j = 0; j < countTables; j++) {
            Node* currCell = new Node;
            currCell->cell = "";
            currCell->next = nullptr;
            RowNode* currRow = tables[j];

            for (int k = 0; k < i && currRow != nullptr; k++) {
                currRow = currRow->nextRow;
            }
            if (currRow != nullptr) {
                Node* columnCell = currRow->cell;
                int targetColumn = numColumns[j];

                for (int z = 0; z < targetColumn - 1 && columnCell != nullptr; z++) {
                    if(columnCell -> next!=nullptr) {
                        columnCell = columnCell->next;
                    }
                    else{
                        columnCell->cell = "";
                    }
                    
                }

                currCell->name = currRow->name;
                currCell->cell = columnCell->cell;
                currCell->next = nullptr;
            }
            else {
                currCell = new Node;
                currCell->name = "";
                currCell->cell = "";
                currCell->next = nullptr;
            }
            if (lastCell == nullptr) {
                newRow->cell = currCell;
            }
            else {
                lastCell->next = currCell;
            }
            lastCell = currCell;
        }
        if (crossTable == nullptr) {
            crossTable = newRow;
        }
        else {
            lastRow->nextRow = newRow;
        }
        lastRow = newRow;
    }
    crossTable = AddTableNames(crossTable, tables, countTables);
    crossTable = AddNumColumns(crossTable, numColumns, countTables);

    return crossTable;
}
RowNode* SelectFromCartesian(RowNode** tables, int countTables) {
    RowNode* crossTable = tables[0];
    for (int i = 1; i < countTables; ++i) {
        RowNode* currentTable = tables[i];
        RowNode* newCrossTable = nullptr;
        RowNode* rowA = crossTable;

        while (rowA != nullptr) {
            RowNode* rowB = currentTable;

            while (rowB != nullptr) {
                RowNode* newRow = new RowNode;
                newRow->cell = nullptr;
                newRow->nextRow = nullptr;

                Node* currCellA = rowA->cell;
                Node* lastCell = nullptr;

                while (currCellA != nullptr) {
                    Node* newCell = new Node;
                    newCell->cell = currCellA->cell;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    }
                    else {
                        lastCell->next = newCell;

                    }
                    lastCell = newCell;
                    currCellA = currCellA->next;
                }

                Node* currCellB = rowB->cell;
                while (currCellB != nullptr) {

                    Node* newCell = new Node;
                    newCell->cell = currCellB->cell;
                    newCell->next = nullptr;

                    if (newRow->cell == nullptr) {
                        newRow->cell = newCell;
                    }
                    else {
                        lastCell->next = newCell;

                    }
                    lastCell = newCell;
                    currCellB = currCellB->next;


                }
                if (newCrossTable == nullptr) {
                    newCrossTable = newRow;
                }
                else {
                    RowNode* lastRow = newCrossTable;
                    while (lastRow->nextRow != nullptr) {
                        lastRow = lastRow->nextRow;
                    }
                    lastRow->nextRow = newRow;
                }
                rowB = rowB->nextRow;
            }
            rowA = rowA->nextRow;
        }
        crossTable = newCrossTable;
    }

    return crossTable;
}
Condition* SplitCondition(const string& filter) {
    Condition* firstElement = nullptr;
    Condition* lastElement = nullptr;
    int begin = 0;
    int end;

    while (begin < filter.length()) {
        end = filter.find("AND", begin);
        int findOR = filter.find("OR", begin);
        if (findOR != string::npos && (end == string::npos || findOR < end)) {
            end = findOR;
        }
        if (end == string::npos) {
            end = filter.length();
        }
        Condition* newNode = new Condition;
        newNode->condition = filter.substr(begin, end - begin);
        newNode->next = nullptr;

        if (end < filter.length()) {
            if (filter.substr(end, 3) == "AND") {
                newNode->oper = "AND";
                end += 3;
            }
            else if (filter.substr(end, 2) == "OR") {
                newNode->oper = "OR";
                end += 2;
            }
            else {
                newNode->oper = "";
            }
        }
        else {
            newNode->oper = "";
        }

        if (firstElement == nullptr) {
            firstElement = newNode;
            lastElement = firstElement;
        }
        else {
            lastElement->next = newNode;
            lastElement = newNode;
        }

        begin = end + 1;
    }
    return firstElement;
}

bool CheckingCondition(RowNode* row, const string& condition) {
    int equalPos = condition.find('=');

    string leftSide = condition.substr(0, equalPos);
    string rightSide = condition.substr(equalPos + 1);

    leftSide.erase(remove(leftSide.begin(), leftSide.end(), ' '), leftSide.end());
    rightSide.erase(remove(rightSide.begin(), rightSide.end(), ' '), rightSide.end());

    if (rightSide[0] == '\'') {
        rightSide.erase(remove(rightSide.begin(), rightSide.end(), '\''), rightSide.end());
        RowNode* newRow = row;
        Node* currCell = newRow->cell;
        int pointPos = condition.find('.');
        string nameTable = condition.substr(0, condition.find('.'));
        int tempPos = condition.find("колонка") + strlen("колонка");
        string numColumnStr = condition.substr(tempPos, equalPos - tempPos - 1);
        int numColumn = stoi(numColumnStr);
        int i = 1;
        while (i != numColumn) {
            currCell = currCell->next;
            i++;
        }
        if (currCell == nullptr) {
            return false;
        }
        if (currCell->cell == rightSide) {
            return true;
        }
        return false;
    }
    else {
        string tableNameA = leftSide.substr(0, leftSide.find('.'));
        int equalPosA = leftSide.find('=');
        int tempPosA = leftSide.find("колонка") + strlen("колонка");
        string numColumnStrA = leftSide.substr(tempPosA, equalPosA - tempPosA - 1);
        int numColumnA = stoi(numColumnStrA);

        string tableNameB = rightSide.substr(0, rightSide.find('.'));
        int equalPosB = rightSide.find('=');
        int tempPosB = rightSide.find("колонка") + strlen("колонка");
        string numColumnStrB = rightSide.substr(tempPosB, rightSide.size() - 1 - tempPosB - 1);
        int numColumnB = stoi(numColumnStrB);
        RowNode* newRow = row;
        Node* currCell = newRow->cell;
        Node* cellA = new Node;
        cellA->cell = "";
        Node* cellB = new Node;
        cellB->cell = "";
        while (currCell != nullptr) {

            if (currCell->name == tableNameA && currCell->numColumn == numColumnA) {
                cellA->cell = currCell->cell;
            }
            if (currCell->name == tableNameB && currCell->numColumn == numColumnB) {
                cellB->cell = currCell->cell;
            }
            currCell = currCell->next;

        }

        if (cellA != nullptr && cellB != nullptr) {
            return cellA->cell == cellB->cell;
        }

        return false;
    }
}
bool CheckingLogicalExpression(RowNode* row, Condition* condition) {
    string result;

    while (condition != nullptr) {
        bool tempResult = CheckingCondition(row, condition->condition);
        result += (tempResult ? "1" : "0");

        if (condition->next != nullptr) {
            result += " " + condition->oper + " ";
        }

        condition = condition->next;
    }

    while (result.find("1 AND 0") != string::npos) {
        result.replace(result.find("1 AND 0"), 7, "0");
    }
    while (result.find("0 AND 1") != string::npos) {
        result.replace(result.find("0 AND 1"), 7, "0");
    }
    while (result.find("0 AND 0") != string::npos) {
        result.replace(result.find("0 AND 0"), 7, "0");
    }
    while (result.find("0 AND 0") != string::npos) {
        result.replace(result.find("1 AND 1"), 7, "1");
    }

    string cleanedResult;
    int pos = 0;
    while (pos < result.length()) {
        int nextOp = result.find("AND", pos);
        if (nextOp == string::npos) {
            cleanedResult += result.substr(pos);
            break;
        }
        cleanedResult += result.substr(pos, nextOp - pos);
        pos = nextOp + 3;
        while (pos < result.length() && result[pos] == ' ') {
            pos++;
        }
    }
    if (cleanedResult.find("1") == string::npos) {
        return false;
    }
    return true;

}
RowNode* FilteringTable(RowNode** select, RowNode** where, int selectSize, int whereSize, int numColumnsSelect[], int numColumnsWhere[], string condition) {
    RowNode* resultTable = nullptr;

    Condition* SplitConditioned = SplitCondition(condition);
    if (selectSize == 1) {
        RowNode* editTable = SelectFromOneTable(select[0], numColumnsSelect);
        RowNode* rateTable = SelectFromOneTable(where[0], numColumnsWhere);

        RowNode* tempEditTable = select[0];

        RowNode* currRow = rateTable;
        while (currRow != nullptr) {
            if (CheckingLogicalExpression(currRow, SplitConditioned)) {

                string listString[1000];
                int i = 0;
                Node* currCell = tempEditTable->cell; 
                while (currCell != nullptr) {
                    listString[i++] = currCell->cell;
                    currCell = currCell->next;
                }
                listString[i] = ""; 
                resultTable = InsertInto(resultTable, listString);
                
            }
            tempEditTable = tempEditTable->nextRow;
            currRow = currRow->nextRow;
        }
        return resultTable;
    }
    else {
        RowNode* tempTableA = SelectFromOneTable(select[0], numColumnsSelect);
        RowNode* tempTableB = SelectFromOneTable(select[1], numColumnsSelect);
        RowNode* rateTable = SelectFromManyTables(where, numColumnsWhere, whereSize);
        RowNode* editTableA;
        RowNode* editTableB;
        RowNode* currRow = rateTable;
        while (currRow != nullptr) {
            if (CheckingLogicalExpression(currRow, SplitConditioned)) {
                string listStringA[1000];
                int i = 0;
                Node* currCellA = tempTableA ->cell; 
                while (currCellA != nullptr) {
                    listStringA[i++] = currCellA->cell;
                    currCellA = currCellA->next;
                }
                listStringA[i] = ""; 
                editTableA = InsertInto(editTableA, listStringA);

                string listStringB[1000];
                int j = 0;
                Node* currCellB = tempTableB ->cell; 
                while (currCellB != nullptr) {
                    listStringB[j++] = currCellB->cell;
                    currCellB = currCellB->next;
                }
                listStringB[j] = ""; 
                editTableB = InsertInto(editTableB, listStringB);
            }
            if(tempTableA->nextRow != nullptr) {
                tempTableA = tempTableA->nextRow;
            }
            if(tempTableB->nextRow != nullptr) {
                tempTableB = tempTableB->nextRow;
            }
            currRow = currRow->nextRow;
        }
        RowNode* tables[] = {editTableA, editTableB};
        RowNode* crossTable = SelectFromCartesian(tables, 2);
        return crossTable;
    }
}
RowNode* DeleteFrom(RowNode* table, Condition* condition) {
    RowNode* currRow = table;
    RowNode* prevRow = nullptr;

    while (currRow != nullptr) {
        if (CheckingLogicalExpression(currRow, condition)) {
            RowNode* nextRow = currRow->nextRow;
            delete currRow;
            if (prevRow == nullptr) {
                table = nextRow;
                currRow = nextRow;
            }
            else {
                prevRow->nextRow = nextRow;
                currRow = nextRow;
            }
        }
        else {
            prevRow = currRow;
            currRow = currRow->nextRow;
        }
    }

    return table;
}
void CreateTable(const string& schemaDir, const string& tableName, ostringstream& toClient) {

    string tableDir = schemaDir + "/" + tableName;
    if (!fs::create_directory(tableDir)) {
        toClient << "Не удалось создать директорию " << tableDir << endl;
        return;
    }

    ofstream csvFile(tableDir + "/1.csv", ios::binary);
    if (!csvFile.is_open()) {
        toClient << "Не удалось создать файл " << tableDir + "/1.csv" << endl;
        return;
    }
    csvFile.close();

    ofstream pkFile(tableDir + "/" + tableName + "_pk_sequence.txt", ios::binary);
    if (!pkFile.is_open()) {
        toClient << "Не удалось создать файл " << tableDir + "/" + tableName + "_pk_sequence.txt" << endl;
        return;
    }
    pkFile << "0";
    pkFile.close();

    ofstream lockFile(tableDir + "/" + tableName + "_lock.txt", ios::binary);
    if (!lockFile.is_open()) {
        toClient << "Не удалось создать файл " << tableDir + "/" + tableName + "_lock.txt" << endl;
        return;
    }
    lockFile << "0";
    lockFile.close();

    toClient << "Таблица " << tableName << " успешно создана в " << tableDir << endl;

    string schemaFilePath = "schema.json";
    ifstream schemaFile(schemaFilePath);
    if (!schemaFile.is_open()) {
        toClient << "Не удалось открыть файл " << schemaFilePath << endl;
        return;
    }

    json schema;
    schemaFile >> schema;
    schemaFile.close();


    schema["structure"][tableName] = json::array();

    ofstream outputFile(schemaFilePath);
    if (outputFile.is_open()) {
        outputFile << schema.dump(4);
        outputFile.close();
    }
    else {
        toClient << "Не удалось записать в файл " << schemaFilePath << endl;
    }

}