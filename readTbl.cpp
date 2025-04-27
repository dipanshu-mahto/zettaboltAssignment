#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

int main() {
    std::ifstream file("C:/Users/dipan/Desktop/zettabolt/ALL_TABLES/customer.tbl");
    std::string line;

    if (!file.is_open()) {
        std::cerr << "Error opening file\n";
        return 1;
    }

    while (std::getline(file, line)) {
        std::vector<std::string> fields;
        std::stringstream ss(line);
        std::string field;

        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }

        // Example: print Customer ID and Name
        if (fields.size() > 2) {
            std::cout << "Customer ID: " << fields[0] << ", Name: " << fields[1] << "\n";
        }
    }

    file.close();
    return 0;
}
