#include<bits/stdc++.h>
using namespace std;

void readConfig(string filename, vector<vector<int>>& adj_matrix) {
    ifstream file(filename);

    vector<vector<int>> rows;
    string line;

    while(getline(file, line)) {
        stringstream input(line);
        char first;
        if(!(input >> first) || first == '#') continue;
        stringstream values(line);
        vector<int> row;
        int cost;
        while(values >> cost) row.push_back(cost);
        rows.push_back(row);
    }

    int n = rows.size()+1;
    adj_matrix.assign(n, vector<int>(n, -1));
    for(int i = 0; i < n; i++) adj_matrix[i][i] = 0;
    for(int i = 0; i < n-1; i++) {
        for(int j = i+1; j < n; j++) {
            adj_matrix[i][j] = rows[i][j-i-1];
            adj_matrix[j][i] = adj_matrix[i][j];
        }
    }
}
int main() {
    vector<vector<int>> adj_matrix;
    
    readConfig("config.txt", adj_matrix);
    int n = adj_matrix.size();
    cout << "number of nodes: " << n;
    cout << "adj_matrix:\n";
    for(int i = 0; i < n; i++) {
        for(int j = 0; j < n; j++) {
            cout << adj_matrix[i][j] << '\t';
        }
        cout << '\n';
    }
    return 0;
}