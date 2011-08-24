/* 
 * File:   BDBIDX.h
 * Author: flachesis
 *
 * Created on 2011年7月3日, 下午 5:37
 */

#ifndef BDBIDX_H
#define	BDBIDX_H

#include <string>
#include <set>
// DO NOT include external headers that are 
// not required by this(BDBIDX.h) header
//
// #include <memory>
// #include <sstream>
// #include <cstring>
#include <cstdio>
#include "bdb/bdb.hpp"

struct hashFunc 
{
	virtual size_t
	operator()(char const *key, size_t len) const = 0;
	virtual ~hashFunc(){}
};

struct default_hash;

class BDBIDX {
public:
    BDBIDX(const char *idx_dir, size_t key_hashing_table_size=50000000, hashFunc* hFn=0);
    virtual ~BDBIDX();
    bool put_key(const char *key, size_t key_len, BDB::AddrType addr);
    bool del_key(const char *key, size_t key_len);
    bool del_key(const char *key, size_t key_len, BDB::AddrType addr);
    size_t get_value(const char *key, size_t key_len, std::set<BDB::AddrType> *addrs);
    bool is_in(const char *key, size_t key_len);

    bool put_key(std::string key, BDB::AddrType addr);
    bool del_key(std::string key);
    bool del_key(std::string key, BDB::AddrType addr);
    size_t get_value(std::string key, std::set<BDB::AddrType> *addrs);
    bool is_in(std::string key);
private:
    BDBIDX(const BDBIDX& orig);
    BDBIDX& operator=(const BDBIDX& orig);

    std::string idx_dir;
    FILE *idx_saving_handle;
    BDB::BehaviorDB *bdb;
    BDB::AddrType *key_hashing_table;
    size_t key_hashing_table_size;
    
    void init_bdbidx(const char *idx_dir, size_t key_hashing_table_size, hashFunc* hFn);
    std::set<BDB::AddrType>* get_key_info(const char *key, size_t key_len, std::string &rec_content);
    hashFunc* BKDRHash;
    static default_hash default_hash_impl_;
    //size_t BKDRHash(const char *str, size_t str_len);
};

#endif	/* BDBIDX_H */

