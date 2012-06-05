/* 
 * File:   BDBIDX.cpp
 * Author: flachesis
 * 
 * Created on 2011年7月3日, 下午 5:37
 */


#include <iostream>
#include <memory>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include "BDBIDX.h"

struct default_hash : hashFunc
{
  size_t
  operator()(char const *key, size_t len) const
  {
    uint32_t hash = 0;
    uint32_t i = 0;
    while (i < len)
    {
      hash = ((hash << 7) + (hash << 1) + hash) + key[i];
      i++;
    }
    return hash;
  
  }

  ~default_hash(){}
};

default_hash BDBIDX::default_hash_impl_;

BDBIDX::BDBIDX(const char *idx_dir, size_t key_hashing_table_size, hashFunc* hFn)
: idx_dir(), idx_saving_handle(0), bdb(0), key_hashing_table(0), BKDRHash(0)
  ,ss(std::ios::in | std::ios::out | std::ios::binary)
  ,key_addr_formater("%08x""%08x""%s"), chunk_addr_formater("%08x""%08x")
{
  int offsets[] = {8,8};
  offsp = new boost::offset_separator(offsets, offsets+2);
  tok = new boost::tokenizer<boost::offset_separator>(rec, *offsp);
  this->init_bdbidx(idx_dir, key_hashing_table_size, hFn);
}


BDBIDX::~BDBIDX() {
  fclose(this->idx_saving_handle);
  replay_reduce();
  delete [] this->key_hashing_table;
  delete this->bdb;
  delete tok;
  delete offsp;
}

void BDBIDX::replay_reduce(){
  std::string idx_saving_file = this->idx_dir + "addr_idx.log.tmp";
  FILE *idx_saving_handle_tmp = fopen(idx_saving_file.c_str(), "wb");
  for(size_t idx_chunk_num = 0; idx_chunk_num < this->key_hashing_table_size; idx_chunk_num++){
    if(this->key_hashing_table[idx_chunk_num] != -1){
      chunk_addr_formater % idx_chunk_num % this->key_hashing_table[idx_chunk_num];
      fwrite(chunk_addr_formater.str().c_str(), 1, chunk_addr_formater.str().size(), idx_saving_handle_tmp);
    }
  }
  fclose(idx_saving_handle_tmp);
  std::string idx_saving_file2 = this->idx_dir + "addr_idx.log";
  remove(idx_saving_file2.c_str());
  rename(idx_saving_file.c_str(), idx_saving_file2.c_str());
}

void BDBIDX::init_bdbidx(
  const char *idx_dir, 
  size_t key_hashing_table_size, 
  hashFunc* hFn)
{
  this->idx_dir = idx_dir;
  if(*(this->idx_dir.end() - 1) != '/'){
    this->idx_dir += "/";
  }
  std::string idx_saving_file = this->idx_dir + "addr_idx.log";

  this->idx_saving_handle = fopen(idx_saving_file.c_str(), "a+b");
  if(0 == idx_saving_handle) {
    std::string msg = "Can't open file ";
    msg += idx_saving_file;
    throw std::runtime_error(msg.c_str());
  }
  
  // init BDB
  BDB::Config bdb_config;
  bdb_config.root_dir = this->idx_dir.c_str();
  bdb_config.min_size = 32;
  bdb_config.beg = 0;
  bdb_config.end = key_hashing_table_size;
  this->bdb = new BDB::BehaviorDB(bdb_config);
  
  // init hash table
  this->key_hashing_table_size = key_hashing_table_size;
  this->key_hashing_table = new BDB::AddrType[this->key_hashing_table_size];
  assert(0 != key_hashing_table && "alloc key_hashing_table failure");
  
  // init hash func
  BKDRHash = (0 == hFn) ? &default_hash_impl_ : hFn ;

  // TODO: the final parameter may overflow
  for(size_t i = 0; i < this->key_hashing_table_size; i++){
    this->key_hashing_table[i] = -1;
  }
  
  // TODO: use line-by-line reading
  // It's proper since the reading is sequential and is invoked 
  // in construction only.
  fseek(this->idx_saving_handle, 0, SEEK_END);
  size_t file_size = ftell(this->idx_saving_handle);
  char *file_content = new char[file_size + 1];
  fseek(this->idx_saving_handle, 0, SEEK_SET);
  fread(file_content, 1, file_size, this->idx_saving_handle);
  file_content[file_size] = '\0'; 
  char *pch = file_content;
  char *pch2 = file_content + file_size;
  unsigned int tmp_value;
  BDB::AddrType tmp_addr = -1;
  while(pch < pch2){
    ss.str("");ss.clear();
    ss.write(pch, 8);
    ss >> std::hex >> tmp_value;
    pch += 8;
    ss.str("");ss.clear();
    ss.write(pch, 8);
    ss >> std::hex >> tmp_addr;
    this->key_hashing_table[tmp_value] = tmp_addr;
    pch += 8;
  }
  delete [] file_content;
}

bool BDBIDX::put_key(const std::string &key, const BDB::AddrType &addr){
  using namespace std;

  size_t idx_chunk_num = (*BKDRHash)(key.c_str(), key.length()) % this->key_hashing_table_size;

  if(this->key_hashing_table[idx_chunk_num] == -1){
    key_addr_formater % addr % key.length() % key;
    try{
      this->key_hashing_table[idx_chunk_num] = this->bdb->put(key_addr_formater.str());
    }catch(...){
      this->key_hashing_table[idx_chunk_num] == -1;
    }
    chunk_addr_formater % idx_chunk_num % this->key_hashing_table[idx_chunk_num];
    fwrite(chunk_addr_formater.str().c_str(), 1, chunk_addr_formater.str().size(), this->idx_saving_handle);
    fflush(this->idx_saving_handle);
    return true;
  }
  
  rec.clear();
  try{
    this->bdb->get(&rec, -1, this->key_hashing_table[idx_chunk_num], 0);
  }catch(...){
    return false;
  }
  auto_ptr<set<BDB::AddrType> > keyinfo(new set<BDB::AddrType>);
  this->get_key_info(key, rec, keyinfo.get());
  if(keyinfo->find(addr) != keyinfo->end()){
    return true;
  }
  key_addr_formater % addr % key.length() % key;
  this->bdb->put(key_addr_formater.str(), this->key_hashing_table[idx_chunk_num], -1);
  return true;
}

bool BDBIDX::del_key(const std::string &key){
  using namespace std;

  size_t idx_chunk_num = (*BKDRHash)(key.c_str(), key.length()) % this->key_hashing_table_size;
  if(this->key_hashing_table[idx_chunk_num] == -1){
    return true;
  }

  rec.clear();
  try{
    this->bdb->get(&rec, -1, this->key_hashing_table[idx_chunk_num], 0);
  }catch(...){
    return false;
  }

  BDB::AddrType tmp_addr = -1;
  unsigned int key_len = 0;
  size_t pos = 0;
  string::iterator it = rec.begin();
  boost::tokenizer<boost::offset_separator>::iterator it2;
  string newrec;

  while(it < rec.end()){
    tok->assign(it, it + 16, *offsp);
    it2 = tok->begin();
    ss.str("");ss.clear();
    ss << *(it2);
    ss >> hex >> tmp_addr;
    ss.str("");ss.clear();
    it2++;
    ss << *(it2);
    ss >> hex >> key_len;
    it += 16 + key_len;
    pos += 16;
    if(rec.compare(pos, key_len, key) != 0){
      key_addr_formater % tmp_addr % key_len % rec.substr(pos, key_len);
      newrec += key_addr_formater.str();
    }
    pos += key_len;
  }
  if(newrec.size() == 0){
    tmp_addr = -1;
    chunk_addr_formater % idx_chunk_num % tmp_addr;
    fwrite(chunk_addr_formater.str().c_str(), 1, chunk_addr_formater.str().size(), this->idx_saving_handle);
    fflush(this->idx_saving_handle);
    try{
      this->bdb->del(this->key_hashing_table[idx_chunk_num]);
    }catch(...){
      this->key_hashing_table[idx_chunk_num] = -1;
      return false;
    }
    this->key_hashing_table[idx_chunk_num] = -1;
  }else{
    this->bdb->update(newrec, this->key_hashing_table[idx_chunk_num]);
  }
  return true;
}

bool BDBIDX::del_key(const std::string &key, const BDB::AddrType &addr){
  using namespace std;

  size_t idx_chunk_num = (*BKDRHash)(key.c_str(), key.length()) % this->key_hashing_table_size;
  if(this->key_hashing_table[idx_chunk_num] == -1){
    return true;
  }
  rec.clear();
  try{
    this->bdb->get(&rec, -1, this->key_hashing_table[idx_chunk_num], 0);
  }catch(...){
    return false;
  }
  
  BDB::AddrType tmp_addr = -1;
  unsigned int key_len = 0;
  size_t pos = 0;
  string::iterator it = rec.begin();
  boost::tokenizer<boost::offset_separator>::iterator it2;
  string newrec;

  while(it < rec.end()){
    tok->assign(it, it + 16, *offsp);
    it2 = tok->begin();
    ss.str("");ss.clear();
    ss << *(it2);
    ss >> hex >> tmp_addr;
    ss.str("");ss.clear();
    it2++;
    ss << *(it2);
    ss >> hex >> key_len;
    it += 16 + key_len;
    pos += 16;
    if(rec.compare(pos, key_len, key) == 0 && tmp_addr == addr){
      pos += key_len;
      newrec += rec.substr(pos);
      break;
    }
    key_addr_formater % tmp_addr % key_len % rec.substr(pos, key_len);
    newrec += key_addr_formater.str();
    pos += key_len;
  }
  if(newrec.size() == 0){
    tmp_addr = -1;
    chunk_addr_formater % idx_chunk_num % tmp_addr;
    fwrite(chunk_addr_formater.str().c_str(), 1, chunk_addr_formater.str().size(), this->idx_saving_handle);
    fflush(this->idx_saving_handle);
    try{
      this->bdb->del(this->key_hashing_table[idx_chunk_num])
    }catch(...){
      this->key_hashing_table[idx_chunk_num] = -1;
      return false;
    }
    this->key_hashing_table[idx_chunk_num] = -1;
  }else{
    this->bdb->update(newrec, this->key_hashing_table[idx_chunk_num]);
  }
  return true;
}

size_t BDBIDX::get_value(const std::string &key, std::set<BDB::AddrType> *addrs){
  size_t idx_chunk_num = (*BKDRHash)(key.c_str(), key.length()) % this->key_hashing_table_size;
  if(this->key_hashing_table[idx_chunk_num] == -1){
    return 0;
  }
  rec.clear();
  try{
    this->bdb->get(&rec, -1, this->key_hashing_table[idx_chunk_num], 0);
  }catch(...){
    return 0;
  }
  this->get_key_info(key, rec, addrs);
  return addrs->size();
}

size_t BDBIDX::get_pool(const std::string &key, boost::unordered_multimap<std::string, BDB::AddrType> *addrs){
  size_t idx_chunk_num = (*BKDRHash)(key.c_str(), key.length()) % this->key_hashing_table_size;
  if(this->key_hashing_table[idx_chunk_num] == -1){
    return 0;
  }
  rec.clear();
  try{
    this->bdb->get(&rec, -1, this->key_hashing_table[idx_chunk_num], 0);
  }catch(...){
    return 0;
  }
  this->get_key_info(rec, addrs);
  return addrs->size();
}

bool BDBIDX::is_in(const std::string &key){
  using namespace std;

  auto_ptr<std::set<BDB::AddrType> > addrs(new std::set<BDB::AddrType>);
  size_t total_rec = this->get_value(key, addrs.get());
  if(total_rec > 0){
    return true;
  }
  return false;
}

bool BDBIDX::put_key(const char *key, size_t key_len, const BDB::AddrType &addr)
{
  std::string key_string(key, key_len);
  return this->put_key(key_string, addr);
}

bool BDBIDX::del_key(const char *key, size_t key_len)
{
  std::string key_string(key, key_len);
  return this->del_key(key_string);
}

bool BDBIDX::del_key(const char *key, size_t key_len, const BDB::AddrType &addr){
  std::string key_string(key, key_len);
  return this->del_key(key_string, addr);
}

size_t BDBIDX::get_value(const char *key, size_t key_len, std::set<BDB::AddrType> *addrs)
{
  std::string key_string(key, key_len);
  return this->get_value(key_string, addrs);
}

size_t BDBIDX::get_pool(const char *key, size_t key_len, boost::unordered_multimap<std::string, BDB::AddrType> *addrs){
  std::string key_string(key, key_len);
  return this->get_pool(key_string, addrs);
}

bool BDBIDX::is_in(const char *key, size_t key_len){
  std::string key_string(key, key_len);
  return this->is_in(key);
}

void BDBIDX::get_key_info(const std::string &rec_content, boost::unordered_multimap<std::string, BDB::AddrType> *addrs){

  using namespace std;
  
  BDB::AddrType tmp_addr = -1;
  unsigned int key_len = 0;
  size_t pos = 0;
  string::const_iterator it = rec_content.begin();
  boost::tokenizer<boost::offset_separator>::iterator it2;
  string key;
  addrs->clear();
  while(it < rec_content.end()){
    tok->assign(it, it + 16, *offsp);
    it2 = tok->begin();
    ss.str("");ss.clear();
    ss << *(it2);
    ss >> hex >> tmp_addr;
    ss.str("");ss.clear();
    it2++;
    ss << *(it2);
    ss >> hex >> key_len;
    it += 16 + key_len;
    pos += 16;
    addrs->insert(pair<string, BDB::AddrType>(rec_content.substr(pos, key_len), tmp_addr));
    pos += key_len;
  }
}

void BDBIDX::get_key_info(const std::string &key, const std::string &rec_content, std::set<BDB::AddrType> *addrs)
{
  using namespace std;
  
  BDB::AddrType tmp_addr = -1;
  unsigned int key_len = 0;
  size_t pos = 0;
  string::const_iterator it = rec_content.begin();
  boost::tokenizer<boost::offset_separator>::iterator it2;
  addrs->clear();
  while(it < rec_content.end()){
    tok->assign(it, it + 16, *offsp);
    it2 = tok->begin();
    ss.str("");ss.clear();
    ss << *(it2);
    ss >> hex >> tmp_addr;
    ss.str("");ss.clear();
    it2++;
    ss << *(it2);
    ss >> hex >> key_len;
    it += 16 + key_len;
    pos += 16;
    if(rec_content.compare(pos, key_len, key) == 0){
      addrs->insert(tmp_addr);
    }
    pos += key_len;
  }
}
