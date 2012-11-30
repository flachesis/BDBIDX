#include <ctime>
#include <cstring>
#include <string>
#include <iostream>
#include <map>
#include <set>
#include <memory>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/variate_generator.hpp>
#include "BDBIDX.h"


struct Pool_hash : hashFunc{
  size_t
    operator()(char const *key, size_t len) const{
      return key[0];
    };
  ~Pool_hash(){};
};


boost::mt19937 gen(std::time(NULL));
size_t addr_count = 1000;

int random_num(int min, int max);
std::string random_key(size_t key_len);
bool test_run1(const char *dir_name);
bool test_run2(const char *dir_name);

int main(int argc, char **argv){
  if(argc != 3){
    std::cerr << argv[0] << " db_dir test_rec_num\n [e.g.] " << argv[0] << " tmp 1000" << std::endl;
    return 0;
  }
  addr_count = atoi(argv[2]);
  std::cerr << "test_run1 start!" << std::endl;
  if(test_run1(argv[1])){
    std::cerr << "test_run1 success!" << std::endl;
  }else{
    std::cerr << "test_run1 fail!" << std::endl;
    return 0;
  }
  std::cerr << "test_run2 start!" << std::endl;
  if(test_run2(argv[1])){
    std::cerr << "test_run2 success!" << std::endl;
  }else{
    std::cerr << "test_run2 fail!" << std::endl;
    return 0;
  }
  return 0;
}

bool test_run2(const char *dir_name){
  Pool_hash p_hash;
  BDBIDX idx(dir_name, 50000000, &p_hash);
  std::multimap<std::string, BDB::AddrType> key_value_map;
  std::multimap<char, BDB::AddrType> key_list;
  std::set<char> char_list;
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
      key_list.insert(std::pair<char, BDB::AddrType>(tmp_key[0], *it));
      char_list.insert(tmp_key[0]);
    }
  }
  {
    bool is_nonerror = true;
    std::multimap<std::string, BDB::AddrType>::iterator it;
    std::cerr << "put start!" << std::endl;
    for(it = key_value_map.begin(); it != key_value_map.end(); it++){
      is_nonerror = idx.put_key(it->first, it->second);
      if(!is_nonerror){
        std::cerr << "test_run2_error: put key-> " << it->first << ", put value-> " << it->second << std::endl;
        return false;
      }
    }
    std::cerr << "put finish!" << std::endl;
  }
  {
    std::cerr << "get pool start!" << std::endl;
    std::auto_ptr<boost::unordered_multimap<std::string, BDB::AddrType> > addrs(new boost::unordered_multimap<std::string, BDB::AddrType>);
    std::set<BDB::AddrType> addrs_list;
    std::pair<std::multimap<char, BDB::AddrType>::iterator, std::multimap<char, BDB::AddrType>::iterator> ret;
    std::multimap<char, BDB::AddrType>::iterator it2;
    boost::unordered_multimap<std::string, BDB::AddrType>::iterator it3;
    size_t count = 0;
    for(std::set<char>::iterator it = char_list.begin(); it != char_list.end(); it++){
      idx.get_pool(&(*it), 1, addrs.get());
      for(it3 = addrs->begin(); it3 != addrs->end(); it3++){
        addrs_list.insert(it3->second);
      }
      ret = key_list.equal_range(*it);
      for(it2 = ret.first; it2 != ret.second; it2++){
        count++;
        if(addrs_list.find(it2->second) == addrs_list.end()){
          std::cerr << "test_run2_error: get pool error!" << std::endl;
          return false;
        }
      }
      if(count != addrs->size()){
        std::cerr << "test_run2_error: get pool size error!" << std::endl;
        return false;
      }
      addrs->clear();
      addrs_list.clear();
      break;
    }
    std::cerr << "get pool finish!" << std::endl;
    for(std::multimap<std::string, BDB::AddrType>::iterator it = key_value_map.begin(); it != key_value_map.end(); it++){
      idx.del_key(it->first);
    }
  }
  return true;
}

bool test_run1(const char *dir_name){
  std::multimap<std::string, BDB::AddrType> key_value_map;
  std::set<std::string> key_list;
  {
    BDBIDX idx(dir_name);
    { // init random address-key pairs
      std::set<BDB::AddrType> addr_list;
      std::string tmp_key;

      for(int i = 0; i < addr_count;){
        auto it = addr_list.insert(random_num(1, 49999999));
        if(it.second == false){
          continue;
        }
        i++;
      }
      for(auto it = addr_list.begin(); it != addr_list.end(); it++){
        tmp_key = random_key(random_num(3, 5));
        key_value_map.insert(std::make_pair(tmp_key, *it));
        key_list.insert(tmp_key);
      }
    }
    { // put address-key pairs 
      bool is_nonerror = true;
      std::cerr << "put start!" << std::endl;
      for(auto it = key_value_map.begin(); it != key_value_map.end(); it++){
        is_nonerror = idx.put_key(it->first, it->second);
        if(!is_nonerror){
          std::cerr << "test_run1_error: put key-> " << it->first << ", put value-> " << it->second << std::endl;
          return false;
        }
      }
      std::cerr << "put finish!" << std::endl;
    }
    { // verify put pairs
      std::cerr << "get value test start!" << std::endl;
      std::multimap<std::string, BDB::AddrType>::iterator it2;
      std::set<BDB::AddrType> addrs;
      size_t addrs_num = 0;
      size_t count = 0;

      for(auto it = key_list.begin(); it != key_list.end(); it++){
        addrs_num = idx.get_value(*it, &addrs);
        if(addrs_num == -1){
          std::cerr << "test_run1_error: get error. get key-> " << *it << std::endl;
          return false;
        }
        auto ret = key_value_map.equal_range(*it);
        count = 0;
        while(ret.first != ret.second) {
          count++;
          if(addrs.find(ret.first->second) == addrs.end()){
            std::cerr << "test_run1_error: addr lost. put key-> " << *it << ", put addr-> " << ret.first->second << std::endl;
            return false;
          }
          ++ret.first;
        }
        if(count != addrs.size()){
          std::cerr << "test_run1_error: size not match!" << std::endl;
          return false;
        }
        addrs.clear();
      }
      std::cerr << "get value test finish!" << std::endl;
    }
    // *** bdb is deconstructed here
  }
  // reconstruction
  BDBIDX idx(dir_name);
  {
    std::cerr << "replay test start!" << std::endl;
    std::set<BDB::AddrType> addrs;
    size_t addrs_num = 0;
    size_t count = 0;

    for(auto it = key_list.begin(); it != key_list.end(); it++){
      addrs_num = idx.get_value(*it, &addrs);
      if(addrs_num == -1){
        std::cerr << "test_run1_error: get error. get key-> " << *it << std::endl;
        return false;
      }
      auto ret = key_value_map.equal_range(*it);
      count = 0;
      while (ret.first != ret.second) {
        count++;
        if(addrs.find(ret.first->second) == addrs.end()){
          std::cerr << "test_run1_error: addr lost. put key-> " << *it << ", put addr-> " << ret.first->second << std::endl;
          return false;
        }
        ++ret.first;
      }
      if(count != addrs.size()){
        std::cerr << "test_run1_error: size not match!" << std::endl;
        return false;
      }
      addrs.clear();
    }
    std::cerr << "replay test finish!" << std::endl;
  }
  // deletion
  {
    std::cerr << "del (key, addr) pair test start!" << std::endl;
    size_t del_num = key_value_map.size() / 2;
    bool is_del_success = false;
    std::multimap<std::string, BDB::AddrType>::iterator it = key_value_map.begin();
    std::auto_ptr<std::set<BDB::AddrType> > addrs(new std::set<BDB::AddrType>);
    for(size_t i = 0; i < del_num; i++){
      is_del_success = idx.del_key(it->first, it->second);
      if(!is_del_success){
        std::cerr << "test_run1_error: del error. del key-> " << it->first << ", del addr-> " << it->second << std::endl;
        return false;
      }
      idx.get_value(it->first, addrs.get());
      if(addrs->size() != 0){
        if(addrs->find(it->second) != addrs->end()){
          std::cerr << "test_run1_error: del fail. del key-> " << it->first << ", cant't del addr-> " << it->second << std::endl;
          return false;
        }
      }
      key_value_map.erase(it);
      it = key_value_map.begin();
      addrs->clear();
    }
    std::cerr << "del (key, addr) pair test finish!" << std::endl;
  }
  {
    std::cerr << "del key's addrs test start!" << std::endl;
    for(std::set<std::string>::iterator it = key_list.begin(); it != key_list.end(); it++){
      idx.del_key(*it);
    }
    std::auto_ptr<std::set<BDB::AddrType> > addrs(new std::set<BDB::AddrType>);
    for(std::set<std::string>::iterator it = key_list.begin(); it != key_list.end(); it++){
      idx.get_value(*it, addrs.get());
      if(addrs->size() != 0){
        std::cerr << "test_run1_error: key map addrs not empty." << std::endl;
        return false;
      }
    }
    std::cerr << "del key's addrs test finish!" << std::endl;
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
