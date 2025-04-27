#include <bits/stdc++.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <thread>
#include <mutex>

using namespace std;

// Mutex for thread-safe operations
std::mutex revenueMutex;

// Structures
struct Customer { int custKey, nationKey; };
struct Order { int orderKey, custKey; string orderDate; };
struct LineItem { int orderKey, suppKey; double extendedPrice, discount; };
struct Supplier { int suppKey, nationKey; };
struct Nation { int nationKey, regionKey; string name; };
struct Region { int regionKey; string name; };

int getCustomerKey(const Customer& c) { return c.custKey; }
int getOrderKey(const Order& o) { return o.orderKey; }
int getSupplierKey(const Supplier& s) { return s.suppKey; }
int getNationKey(const Nation& n) { return n.nationKey; }
int getRegionKey(const Region& r) { return r.regionKey; }

// Data storage
unordered_map<int, Customer> customers;
unordered_map<int, Order> orders;
vector<LineItem> lineitems;
unordered_map<int, Supplier> suppliers;
unordered_map<int, Nation> nations;
unordered_map<int, Region> regions;

// Helper function: Proper CSV splitting (handling quotes)
vector<string> splitCSVLine(const string& line) {
    vector<string> result;
    string field;
    bool inQuotes = false;
    for (char ch : line) {
        if (ch == '"') {
            inQuotes = !inQuotes;
        } else if (ch == ',' && !inQuotes) {
            result.push_back(field);
            field.clear();
        } else {
            field += ch;
        }
    }
    result.push_back(field);
    return result;
}

// Parsing functions
bool parseCustomer(const string& line, Customer& c) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 6) return false;
    try {
        c.custKey = stoi(fields[0]);
        c.nationKey = stoi(fields[3]);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseOrder(const string& line, Order& o) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 5) return false;
    try {
        o.orderKey = stoi(fields[0]);
        o.custKey = stoi(fields[1]);
        o.orderDate = fields[4];
        return true;
    } catch (...) {
        return false;
    }
}

bool parseLineItem(const string& line, LineItem& l) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 6) return false;
    try {
        l.orderKey = stoi(fields[0]);
        l.suppKey = stoi(fields[2]);
        l.extendedPrice = stod(fields[5]);
        l.discount = stod(fields[6]);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseSupplier(const string& line, Supplier& s) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 3) return false;
    try {
        s.suppKey = stoi(fields[0]);
        s.nationKey = stoi(fields[3]);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseNation(const string& line, Nation& n) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 3) return false;
    try {
        n.nationKey = stoi(fields[0]);
        n.name = fields[1];
        n.regionKey = stoi(fields[2]);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseRegion(const string& line, Region& r) {
    auto fields = splitCSVLine(line);
    if (fields.size() < 2) return false;
    try {
        r.regionKey = stoi(fields[0]);
        r.name = fields[1];
        return true;
    } catch (...) {
        return false;
    }
}

// Generic loader
template<typename T>
void loadCSV(const string& filename, unordered_map<int, T>& mapData, bool (*parseFunc)(const string&, T&), int (*getKey)(const T&)) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Cannot open file: " << filename << endl;
        return;
    }
    string line;
    while (getline(file, line)) {
        T obj;
        if (parseFunc(line, obj)) {
            mapData[getKey(obj)] = obj;
        }
    }
    file.close();
}


// LineItem special loader (vector)
void loadLineItems(const string& filename, bool (*parseFunc)(const string&, LineItem&)) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Cannot open file: " << filename << endl;
        return;
    }
    string line;
    while (getline(file, line)) {
        LineItem l;
        if (parseFunc(line, l)) {
            lineitems.push_back(l);
        }
    }
    file.close();
}

// Load all data
void loadAllData() {
    thread t1(loadCSV<Customer>, "All_TABLES/customer.csv", ref(customers), parseCustomer,getCustomerKey);
    thread t2(loadCSV<Order>, "All_TABLES/orders.csv", ref(orders), parseOrder,getOrderKey);
    thread t3(loadLineItems, "All_TABLES/lineitem.csv", parseLineItem);
    thread t4(loadCSV<Supplier>, "All_TABLES/supplier.csv", ref(suppliers), parseSupplier,getSupplierKey);
    thread t5(loadCSV<Nation>, "All_TABLES/nation.csv", ref(nations), parseNation,getNationKey);
    thread t6(loadCSV<Region>, "All_TABLES/region.csv", ref(regions), parseRegion,getRegionKey);

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
    cout << "Customers loaded: " << customers.size() << endl;
cout << "Orders loaded: " << orders.size() << endl;
cout << "LineItems loaded: " << lineitems.size() << endl;
cout << "Suppliers loaded: " << suppliers.size() << endl;
cout << "Nations loaded: " << nations.size() << endl;
cout << "Regions loaded: " << regions.size() << endl;

cout<<"\n\n\n";
}

// Query execution
void runQuery5() {
    unordered_map<int, string> regionMap;
    for (auto& [id, region] : regions) {
        if (region.name == "ASIA")
            regionMap[id] = region.name;
    }

    unordered_map<int, string> nationMap;
    for (auto& [id, nation] : nations) {
        if (regionMap.count(nation.regionKey))
            nationMap[id] = nation.name;
    }

    unordered_map<int, int> supplierMap;
    for (auto& [id, supplier] : suppliers) {
        if (nationMap.count(supplier.nationKey))
            supplierMap[id] = supplier.nationKey;
    }

    unordered_map<int, string> orderDateMap;
    for (auto& [id, order] : orders) {
        orderDateMap[id] = order.orderDate;
    }

    unordered_map<string, double> nationRevenue;

    for (auto& line : lineitems) {
        if (supplierMap.count(line.suppKey) && orderDateMap.count(line.orderKey)) {
            string orderDate = orderDateMap[line.orderKey];
            if (orderDate >= "1994-01-01" && orderDate < "1995-01-01") {
                int nationKey = supplierMap[line.suppKey];
                string nationName = nationMap[nationKey];
                nationRevenue[nationName] += line.extendedPrice * (1.0 - line.discount);
            }
        }
    }

    cout << "Nation\tRevenue" << endl;
    for (auto& [nation, revenue] : nationRevenue) {
        cout << nation << "  " << fixed << setprecision(2) << revenue << endl;
    }
}

// Main
int main() {
    loadAllData();
    runQuery5();
    return 0;
}
