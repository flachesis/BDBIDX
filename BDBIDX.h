/* 
 * File:   BDBIDX.h
 * Author: flachesis
 *
 * Created on 2011年7月3日, 下午 5:37
 */

#ifndef BDBIDX_H
#define	BDBIDX_H

#include <string>
#include <sstream>
#include <set>
#include <memory>
#include <cstring>
#include <cstdio>
#include "common.hpp"
#include "bdb.hpp"

class BDBIDX {
public:
    BDBIDX(const char *idx_dir);
    BDBIDX(const char *idx_dir, size_t key_hashing_table_size);
    BDBIDX(const BDBIDX& orig);
    virtual ~BDBIDX();
    bool put_key(const char *key, size_t key_len, BDB::AddrType addr);
    bool del_key(const char *key, size_t key_len);
    bool del_key(const char *key, size_t key_len, BDB::AddrType addr);
    std::set<BDB::AddrType>* get_addrs(const char *key, size_t key_len);
private:
    std::string idx_dir;
    FILE *idx_saving_handle;
    BDB::BehaviorDB *bdb;
    BDB::AddrType *key_hashing_table;
    size_t key_hashing_table_size;
    BDB::Config bdb_config;
    
    void init_bdbidx(const char *idx_dir, size_t key_hashing_table_size);
    std::set<BDB::AddrType>* get_key_info(const char *key, size_t key_len, std::string &rec_content);
    size_t BKDRHash(const char *str, size_t str_len);
};

#endif	/* BDBIDX_H */

