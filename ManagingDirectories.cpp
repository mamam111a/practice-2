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

void UpdatePrimaryKey(const string& schemaName, const string& tableName, RowNode* table, ostringstream& toClient) {
    int i = 0;
    RowNode* currentRow = table;
    while (currentRow != nullptr) {
        i++;
        currentRow = currentRow->nextRow;
    }
    string pkFilePath = schemaName + "/" + tableName + "/" + tableName + "_pk_sequence.txt";

    ofstream pkFile(pkFilePath, ios::binary);
    if (pkFile.is_open()) {
        pkFile << i;
        pkFile.close();
    }
    else {
        toClient << "Не удалось открыть файл для обновления: " << pkFilePath << endl;
    }
}
void ReadConfiguration(const string& filename, ostringstream& toClient) {
    ifstream file(filename);
    if (!file.is_open()) {
        toClient << "Не удалось открыть файл " << filename << endl;
        return;
    }

    json config;
    file >> config;

    string schemaName = config["name"];
    int tuplesLimit = config["tuples_limit"];
    fs::create_directory(schemaName);

    for (const auto& table : config["structure"].items()) {
        string tableName = table.key();
        fs::create_directory(schemaName + "/" + tableName);
        string csvFilePath = schemaName + "/" + tableName + "/1.csv";

        ofstream csvFile(csvFilePath, ios::binary);
        if (!csvFile.is_open()) {
            toClient << "Ошибка открытия файла для записи: " << csvFilePath << endl;
            continue;
        }
        WriteUtf8BOM(csvFile);

        const auto& columns = table.value();

        if (columns.size() > 0) {
            const auto& headers = columns[0];
            for (const auto& header : headers) {
                csvFile << header.get<string>() << ",";
            }
            csvFile.seekp(-1, ios_base::cur);
            csvFile << "\n";

            for (size_t i = 1; i < columns.size(); ++i) {
                const auto& row = columns[i];
                for (const auto& value : row) {
                    if (!(csvFile << value.get<string>() << ",")) {
                        toClient << "Ошибка записи в файл: " << csvFilePath << endl;
                    }
                }
                csvFile.seekp(-1, ios_base::cur);
                csvFile << "\n";
            }
        }
        else {
            toClient << "Предупреждение: таблица " << tableName << " пустая" << endl;
        }

        csvFile.close();

        int rowCount = countRowsInCSV(csvFilePath, toClient);
        ofstream pkFile(schemaName + "/" + tableName + "/" + tableName + "_pk_sequence.txt", ios::binary);
        if (pkFile.is_open()) {
            pkFile << rowCount;
            pkFile.close();
        }

        ofstream lockFile(schemaName + "/" + tableName + "/" + tableName + "_lock.txt", ios::binary);
        if (lockFile.is_open()) {
            lockFile << 0;
            lockFile.close();
        }
    }
    
    toClient << "Директории были успешно созданы!" << endl;
}
void incrementSequence(const string& tableName, ostringstream& toClient) {
    string fileName = string("Схема 1") + "/" + tableName + "/" + tableName + "_pk_sequence.txt";

    ifstream inputFile(fileName);
    if (!inputFile) {
        toClient << "Ошибка открытия файла для чтения." << endl;
        return;
    }

    string currValueStr;
    inputFile >> currValueStr;
    inputFile.close();
    int currValue = stoi(currValueStr);
    currValue++;
    ofstream outputFile(fileName);
    if (!outputFile) {
        toClient << "Ошибка открытия файла для записи." << endl;
        return;
    }

    outputFile << currValue;
    outputFile.close();

}
void Lock(const string& tableName, bool parameter, ostringstream& toClient) {
    string fileName = string("Схема 1") + "/" + tableName + "/" + tableName + "_lock.txt";

    ifstream inputFile(fileName);
    if (!inputFile) {
        toClient << "Ошибка открытия файла для чтения." << endl;
        return;
    }

    string currValueStr;
    inputFile >> currValueStr;
    inputFile.close();
    int currValue = stoi(currValueStr);
    if(parameter) {
        currValue++;
    }
    else{
        currValue--;
    }
    
    ofstream outputFile(fileName);
    if (!outputFile) {
        toClient << "Ошибка открытия файла для записи." << endl;
        return;
    }

    outputFile << currValue;
    outputFile.close();

}

void RewriteTableSchema(const json& newSchema, const string& schemaFilePath, const string& tableName, ostringstream& toClient) {
    ifstream schemaFile(schemaFilePath);
    if (!schemaFile.is_open()) {
        toClient << "Не удалось открыть файл схемы: " << schemaFilePath << endl;
        return;
    }

    json schemaData;
    schemaFile >> schemaData;
    schemaFile.close();


    if (!schemaData["structure"].contains(tableName)) {
        toClient << "Таблица не найдена: " << tableName << endl;
        return;
    }

    schemaData["structure"][tableName] = newSchema;

    ofstream outFile(schemaFilePath);
    if (outFile.is_open()) {
        outFile << schemaData.dump(4); 
        outFile.close();
    }
    else {
        toClient << "Не удалось записать в файл: " << schemaFilePath << endl;
    }
}
void AddColumnsInSchemaJson(const json& newColumns, const string& schemaFilePath, const string& tableName, ostringstream& toClient) {
    ifstream schemaFile(schemaFilePath);
    if (!schemaFile.is_open()) {
        toClient << "Не удалось открыть файл схемы: " << schemaFilePath << endl;
        return;
    }

    json schemaData;
    schemaFile >> schemaData;
    schemaFile.close();

    if (!schemaData["structure"].contains(tableName)) {
        toClient << "Таблица не найдена: " << tableName << endl;
        return;
    }
    if (!newColumns.is_array()) {
        toClient << "Данные колонок должны быть массивом." << endl;
        return;
    }

    for (const auto& column : newColumns) {
        if (column.is_array()) {
            json newRow = json::array();
            for (const auto& value : column) {
                newRow.push_back(value);
            }
            schemaData["structure"][tableName].push_back(newRow);
        }
    }

    ofstream outFile(schemaFilePath);
    if (outFile.is_open()) {
        outFile << schemaData.dump(4);
        outFile.close();
    }
    else {
        toClient << "Не удалось записать в файл: " << schemaFilePath << endl;
    }

    toClient << "Новая строка успешно добавлена! " << endl;
}

