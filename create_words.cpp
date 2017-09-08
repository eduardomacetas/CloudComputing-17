#include <iostream>
#include <vector>
#include <fstream>
#include <stdlib.h> 
using namespace std;

int main(){
    int num_words=466544;
    vector<string> data;
    data.resize(num_words);
    int index;
    string word;

    //cargar data
    for (int i = 0; i < num_words; ++i){
    cin>>word;
    data[i]=word;
}

cout<<"data cargada"<<endl;
ofstream file("10gb.txt", ios::app);

long int i=0;
while(i<1e9){
//while(i<1e9*7){
        index=rand()%num_words;
        file<<data[index]<<endl;
        i++;
    }
    return 0;
}

// g++ create_words.cpp -o a
// ./a < words.txt