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


json ConvertTableToJson(RowNode* table) {
    json jsonData = json::array();

    while (table != nullptr) {
        json rowJson;
        Node* currNode = table->cell;
        while (currNode != nullptr) {
            rowJson.push_back(currNode->cell);
            currNode = currNode->next;
        }
        jsonData.push_back(rowJson);
        table = table->nextRow;
    }

    return jsonData;
}
RowNode* ConvertJsonToTable(const json& jsonData, const string& tableName, ostringstream& toClient) {
    RowNode* table = nullptr;
    if (jsonData.contains("structure") && jsonData["structure"].is_object()) {
        if (jsonData["structure"].contains(tableName)) {
            const auto& tableData = jsonData["structure"][tableName];
            if (tableData.is_array()) {
                for (const auto& row : tableData) {
                    if (row.is_array()) {
                        string listString[1000]; 
                        int i= 0;
                        for (const auto& cell : row) {
                            if (i < 1000) { 
                                listString[i++] = cell.get<string>();
                            }
                        }

                        table = InsertInto(table, listString);
                    }
                    else {
                        toClient << "Ошибка: строка не является массивом." << endl;
                    }
                }
            }
            else {
                toClient << "Ошибка: данные для таблицы не являются массивом." << endl;
            }
        }
        else {
            toClient << "Ошибка: таблица '" << tableName << "' не найдена в данных." << endl;
        }
    }
    else {
        toClient << "Ошибка: структура не найдена или не является объектом." << endl;
    }

    return table;
}

json ReadCSVToJson(const string& filePath, ostringstream& toClient) {
    if (!ifstream(filePath)) {
        toClient << "Файл не найден: " << filePath << endl;
        return -1;
    }

    ifstream csvFile(filePath);
    json jsonData;

    if (!csvFile.is_open()) {
        toClient << "Не удалось открыть файл для чтения: " << filePath << endl;
        return jsonData;
    }
    string line;
    while (getline(csvFile, line)) {

        if (line == "") {
            jsonData = json::array({ json::array() });
            return jsonData;
        }

        stringstream ss(line);
        json rowJson = json::array();
        string value;

        while (getline(ss, value, ',')) {
            value.erase(0, value.find_first_not_of(" \n\r\t"));
            value.erase(value.find_last_not_of(" \n\r\t") + 1);
            if (!value.empty()) {
                rowJson.push_back(value);
            }
        }

        if (!rowJson.empty()) {
            jsonData.push_back(rowJson);
        }
    }

    csvFile.close();
    return jsonData;
}
void WriteJsonToCSV(const string& filePath, const json& jsonData, ostringstream& toClient) {
    ofstream csvFile(filePath, ios::app); 
    if (csvFile.is_open()) {
        for (auto& row : jsonData) {
            for (int i = 0; i < row.size(); ++i) {
                csvFile << row[i].get<string>();
                if (i < row.size() - 1) {
                    csvFile << ","; 
                }
            }
            csvFile << "\n";
        }
        csvFile.close();
    } else {
        toClient << "Не удалось открыть файл для записи: " << filePath << endl;
    }
}
void RewriteCSVbyJson(const string& filePath, const json& jsonData, ostringstream& toClient) {
    ofstream csvFile(filePath);
    if (csvFile.is_open()) {
        WriteUtf8BOM(csvFile);
        for (const auto& row : jsonData) {
            if (row.is_array()) {
                for (size_t i = 0; i < row.size(); ++i) {
                    csvFile << row[i].get<string>();
                    if (i < row.size() - 1) {
                        csvFile << ",";
                    }
                }
                csvFile << "\n";
            }
        }

        csvFile.close();
    }
    else {
        toClient << "Не удалось открыть файл для записи: " << filePath << endl;
    }
}