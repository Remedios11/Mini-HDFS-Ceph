#include<iostream>
#include<assert.h>
#include"MemTable.h"
using namespace std;
using namespace mini_storage;
int main()
{
	MemTable mt;

	// --- 测试1：存入数据(Put) ---
	cout << "puting..." << endl;
	mt.Put("name", "Remedios");
	mt.Put("age", "20");
	mt.Put("city", "nanjing");

	// --- 测试2：统计数据(Count&Size) ---
	cout << "number: " << mt.Count() << endl;
	cout << "size:" << mt.Size() << endl;

	// ---测试3：读取数据(Get) ---
	string val;
	if (mt.Get("name", &val)) {
		cout << "success find:" << val << endl;
	}
	else {
		cout << "failed:not find name" << endl;
	}
	//测试读取不存在的key
	if (!mt.Get("hobby", &val)) {
		cout << "right:not to find unexist hobby" << endl;
	}

	// ---测试4：遍历数据(Iterator) ---
	cout << " ---traverse--- " << endl;
	auto iter = mt.NewIterator();
	while (iter->Valid()) {
		cout << iter->Key() << "=>" << iter->Value() << endl;
		iter->Next();
	}
	delete iter;

	// ---测试5：清空(Clear) ---
	mt.Clear();
	cout << "number:" << mt.Count() << endl;//应该是0

	cout << "finish" << endl;
	return 0;
}