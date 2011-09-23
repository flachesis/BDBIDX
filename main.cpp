#include <ctime>
#include <string>
#include <iostream>
#include <map>
#include <set>
#include <memory>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/variate_generator.hpp>
#include "BDBIDX.h"

boost::mt19937 gen(std::time(NULL));

int random_num(int min, int max);
std::string random_key(size_t key_len);
bool test_run1(const char *dir_name);

int main(void){
	if(test_run1("tmp")){
		std::cout << "test_run1 success!" << std::endl;
	}
	return 0;
}

bool test_run1(const char *dir_name){
	BDBIDX idx(dir_name);
	size_t addr_count = 10000000;
	std::multimap<std::string, BDB::AddrType> key_value_map;
	std::set<std::string> key_list;
	{
		std::set<BDB::AddrType> addr_list;
		{
			std::pair<std::set<BDB::AddrType>::iterator, bool> it;
			for(int i = 0; i < addr_count;){
				it = addr_list.insert(random_num(1, 49999999));
				if(it.second == false){
					continue;
				}
				i++;
			}
		}
		std::string tmp_key;
		std::set<BDB::AddrType>::iterator it;
		for(it = addr_list.begin(); it != addr_list.end(); it++){
			tmp_key = random_key(random_num(3, 5));
			key_value_map.insert(std::pair<std::string, BDB::AddrType>(tmp_key, *it));
			key_list.insert(tmp_key);
		}
	}
	{
		bool is_nonerror = true;
		std::multimap<std::string, BDB::AddrType>::iterator it;
		for(it = key_value_map.begin(); it != key_value_map.end(); it++){
			is_nonerror = idx.put_key(it->first, it->second);
			if(!is_nonerror){
				std::cerr << "test_run1_error: put key-> " << it->first << ", put value-> " << it->second << std::endl;
				return false;
			}
		}
	}
	std::set<std::string>::iterator it;
	std::pair<std::multimap<std::string, BDB::AddrType>::iterator, std::multimap<std::string, BDB::AddrType>::iterator> ret;
	std::multimap<std::string, BDB::AddrType>::iterator it2;
	std::auto_ptr<std::set<BDB::AddrType> > addrs(new std::set<BDB::AddrType>);
	size_t addrs_num;
	for(it = key_list.begin(); it != key_list.end(); it++){
		addrs_num = idx.get_value(*it, addrs.get());
		ret = key_value_map.equal_range(*it);
		for(it2=ret.first; it2!=ret.second; ++it2){
			if(addrs->find(it2->second) == addrs->end()){
				std::cerr << "test_run1_error: addr lost. put key-> " << *it << ", put value-> " << it2->second << std::endl;
				return false;
			}
		}
	}
	return true;
}

std::string random_key(size_t key_len){
	std::string key_rnd;
	char tmp_char;
	for(size_t i = 0; i < key_len; i++){
		tmp_char = random_num(33, 126);
		key_rnd.push_back(tmp_char);
	}
	return key_rnd;
}

int random_num(int min, int max){
	boost::uniform_int<> num(min,max);
	boost::variate_generator<boost::mt19937&, boost::uniform_int<> > die(gen, num);
	return die();
}
