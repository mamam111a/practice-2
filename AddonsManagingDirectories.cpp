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


void WriteUtf8BOM(ofstream& file) {
    const char bom[] = { '\xEF', '\xBB', '\xBF' };
    file.write(bom, sizeof(bom));
}

int countRowsInCSV(const string& csvFilePath, ostringstream& toClient) {
    int i = 0;
    ifstream csvFile(csvFilePath, ios::binary); 
    if (csvFile.is_open()) {
        string line;
    
        char bom[3] = {0};
        csvFile.read(bom, 3); 
        if (bom[0] == (char)0xEF && bom[1] == (char)0xBB && bom[2] == (char)0xBF) {
            csvFile.seekg(3, ios::beg);
        } 
        else {
            csvFile.seekg(0, ios::beg); //beg указывает что чтение идет с начала файла
        }
        while (getline(csvFile, line)) {
            i++; 
        }
        csvFile.close();
    } else {
        toClient << "Ошибка открытия файла: " << csvFilePath << endl;
    }

    return i;
}
int getNextCsv(const string& tableName, ostringstream& toClient) {
    string fileName = "Схема 1/" + tableName + "/" + tableName + "_lock.txt";

    ifstream lockFile(fileName);
    if (!lockFile) {
        toClient << "Ошибка открытия файла: " << fileName << endl;
        return -1;
    }

    int lineCount = 0;
    string line;
    while (getline(lockFile, line)) {
        lineCount++;
    }
    lockFile.close();

    int maxIndex = 0;
    for (const auto& entry : filesystem::directory_iterator("Схема 1/" + tableName)) {
        if (entry.path().extension() == ".csv") {
            string filename = entry.path().stem().string();
            int index = stoi(filename);
            maxIndex = max(maxIndex, index);
        }
    }

    return (lineCount <= 1000) ? maxIndex : (maxIndex + 1);
}