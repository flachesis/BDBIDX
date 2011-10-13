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
#include <sstream>
#include <boost/unordered_map.hpp>
#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
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
	bool put_key(const char *key, size_t key_len, const BDB::AddrType &addr);
	bool del_key(const char *key, size_t key_len);
	bool del_key(const char *key, size_t key_len, const BDB::AddrType &addr);
	size_t get_value(const char *key, size_t key_len, std::set<BDB::AddrType> *addrs);
	size_t get_pool(const char *key, size_t key_len, boost::unordered_multimap<std::string, BDB::AddrType> *addrs);
	bool is_in(const char *key, size_t key_len);

	bool put_key(const std::string &key, const BDB::AddrType &addr);
	bool del_key(const std::string &key);
	bool del_key(const std::string &key, const BDB::AddrType &addr);
	size_t get_value(const std::string &key, std::set<BDB::AddrType> *addrs);
	size_t get_pool(const std::string &key, boost::unordered_multimap<std::string, BDB::AddrType> *addrs);
	bool is_in(const std::string &key);
private:
	BDBIDX(const BDBIDX& orig);
	BDBIDX& operator=(const BDBIDX& orig);

	std::string idx_dir;
	FILE *idx_saving_handle;
	BDB::BehaviorDB *bdb;
	BDB::AddrType *key_hashing_table;
	size_t key_hashing_table_size;
	std::stringstream ss;
	std::string rec;
	boost::format key_addr_formater;
	boost::format chunk_addr_formater;
	boost::offset_separator *offsp;
	boost::tokenizer<boost::offset_separator> *tok;
    
	void replay_reduce();
	void init_bdbidx(const char *idx_dir, size_t key_hashing_table_size, hashFunc* hFn);
	void get_key_info(const std::string &key, const std::string &rec_content, std::set<BDB::AddrType> *addrs);
	void get_key_info(const std::string &rec_content, boost::unordered_multimap<std::string, BDB::AddrType> *addrs);
	hashFunc* BKDRHash;
	static default_hash default_hash_impl_;
	//size_t BKDRHash(const char *str, size_t str_len);
};

#endif	/* BDBIDX_H */

