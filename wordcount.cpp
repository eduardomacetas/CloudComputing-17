//	g++ word-count.cpp -o m -std=c++11 -pthread
//	time ./m < words.txt
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <thread>
using namespace std;
int num_words=466544;
unordered_map<string,int> count;//diccionario ingles

void load_dic(){//cargar diccionario en una tabla hash contador inicial=0 para c/palabra/
	string word;
	for (int i = 0; i < num_words; ++i){
		cin>>word;
		count[word]=0;
	}
	cout<<"diccionario cargado"<<endl;
}

void task1(string filename){
	string word;
    ifstream file1(filename);
	while(file1 >> word){
	    ++count[word];
	}
}


int main(){



	load_dic();//cargar diccionario

	thread t1(task1,"1gb01.txt");
	thread t2(task1,"1gb02.txt");
	thread t3(task1,"1gb03.txt");
	thread t4(task1,"1gb04.txt");
	thread t5(task1,"1gb05.txt");
	thread t6(task1,"1gb06.txt");
	thread t7(task1,"1gb07.txt");
	thread t8(task1,"1gb08.txt");
	thread t9(task1,"1gb09.txt");
	thread t10(task1,"1gb010.txt");

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
    t7.join();
    t8.join();
	t9.join();
	t10.join();
	
	//mostrar solo 20
	int i=0;
	for ( auto it = count.begin(); it!= count.end(); ++it ){
		++i;
      	cout<< it->first << ":" << it->second<<endl;
      	if(i>20){break;} 
	}

	//para guardar las palabras en un nuevo archivo
	
	for(auto elem:count)
	{
		std:cout << elem.first<<" "<<elem.second<<"\n";
	}
	return 0;
}