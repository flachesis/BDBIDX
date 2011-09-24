/* 
 * File:   BDBIDX.cpp
 * Author: flachesis
 * 
 * Created on 2011年7月3日, 下午 5:37
 */


#include <iostream>
#include <memory>
#include <sstream>
#include <cstring>
#include <cassert>
#include <stdexcept>

#include "boost/lexical_cast.hpp"
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/newline.hpp>
#include <boost/tokenizer.hpp>

#include "BDBIDX.h"

#define MAX_STR_LEN 1048576

struct default_hash : hashFunc
{
	size_t
	operator()(char const *key, size_t len) const
	{
		size_t hash = 0;
		size_t i = 0;
		while (i < len)
		{
			hash = ((hash << 7) + 3) + key[i];
			i++;
		}
		return hash;
	
	}

	~default_hash(){}
};

default_hash BDBIDX::default_hash_impl_;

BDBIDX::BDBIDX(const char *idx_dir, size_t key_hashing_table_size, hashFunc* hFn)
: idx_dir(), idx_saving_handle(0), bdb(0), key_hashing_table(0), BKDRHash(0)
{
	this->init_bdbidx(idx_dir, key_hashing_table_size, hFn);
}


BDBIDX::~BDBIDX() {
	delete [] this->key_hashing_table;
	delete this->bdb;
	fclose(this->idx_saving_handle);
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
	memset(this->key_hashing_table, -1, 
		sizeof(BDB::AddrType) * this->key_hashing_table_size);
	
	// TODO: use line-by-line reading
	// It's proper since the reading is sequential and is invoked 
	// in construction only.
	if(this->idx_saving_handle != NULL){
		size_t file_size = ftell(this->idx_saving_handle);
		char *file_content = new char[file_size + 1];
		fseek(this->idx_saving_handle, 0, SEEK_SET);
		fread(file_content, 1, file_size, this->idx_saving_handle);
		file_content[file_size] = '\0'; 
		char *pch = strtok(file_content, "\n");
		unsigned int tmp_value;
		BDB::AddrType tmp_addr = -1;
		while(pch != NULL){
			sscanf(pch, "%u,%u", &tmp_value, &tmp_addr);
			this->key_hashing_table[tmp_value] = tmp_addr;
			pch = strtok(NULL, "\n");
		}
		delete [] file_content;
	}
}

bool BDBIDX::put_key(std::string key, BDB::AddrType addr){
	this->put_key(key.c_str(), key.size(), addr);
}

bool BDBIDX::del_key(std::string key){
	this->del_key(key.c_str(), key.size());
}

bool BDBIDX::del_key(std::string key, BDB::AddrType addr){
	this->del_key(key.c_str(), key.size(), addr);
}

size_t BDBIDX::get_value(std::string key, std::set<BDB::AddrType> *addrs){
	this->get_value(key.c_str(), key.size(), addrs);
}

size_t BDBIDX::get_pool(std::string key, boost::unordered_multimap<std::string, BDB::AddrType> *addrs){
	this->get_pool(key.c_str(), key.size(), addrs);
}

bool BDBIDX::is_in(std::string key){
	return this->is_in(key.c_str(), key.size());
}

bool BDBIDX::put_key(const char *key, size_t key_len, BDB::AddrType addr)
{
	// Yes, you can do that
	using namespace std;
    using boost::lexical_cast;
    using BDB::AddrType;

    assert(key_len <= MAX_STR_LEN && "Key length exceeds maximum length.");

	size_t idx_chunk_num = (*BKDRHash)(key, key_len) % key_hashing_table_size;
	
    string buf;
    
	if(this->key_hashing_table[idx_chunk_num] == -1){
		
        buf = lexical_cast<string>(key_len);
        buf += ",";
        buf.append(key, key_len);
        buf += ",";
        buf += lexical_cast<string>(addr);
        buf += "\n";
        
		key_hashing_table[idx_chunk_num] = bdb->put(buf);

		if(-1 == key_hashing_table[idx_chunk_num])
			return false;
		
        buf = lexical_cast<string>(idx_chunk_num);
        buf += ",";
        buf += lexical_cast<string>(key_hashing_table[idx_chunk_num]);
        buf += "\n";
        
        fwrite(buf.c_str(), 1, buf.size(), idx_saving_handle);
		fflush(idx_saving_handle);
		return true;
	}
	
	if(-1 == bdb->get(&buf, -1, key_hashing_table[idx_chunk_num], 0))
		return false;
	
	
	auto_ptr<set<BDB::AddrType> > keyinfo( get_key_info(key, key_len, buf) );
	if(keyinfo->find(addr) != keyinfo->end())
		return true;
	
    
    buf = lexical_cast<string>(key_len);
    buf += ",";
    buf.append(key, key_len);
    buf += ",";
    buf += lexical_cast<string>(addr);
    buf += "\n";
	bdb->put(buf, key_hashing_table[idx_chunk_num], BDB::npos);
    
   	return true;
}

bool BDBIDX::del_key(const char *key, size_t key_len)
{
	using namespace std;

	size_t idx_chunk_num = (*BKDRHash)(key, key_len) % this->key_hashing_table_size;
	if(this->key_hashing_table[idx_chunk_num] == -1){
		return true;
	}

	auto_ptr<string> rec(new string);
	size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
	if(already_reading == -1){
		return false;
	}

	size_t tmpos1 = 0;
	size_t tmpos2 = 0;
	size_t tmpos3 = 0;
	size_t tmp_value = 0;
	auto_ptr<stringstream> ss(new stringstream(ios::in | ios::out | ios::binary));
	string tmp_value_string = "";
	auto_ptr<string> newrec(new string);

	while((tmpos2 = rec->find(",", tmpos1)) != string::npos){
		tmp_value_string = rec->substr(tmpos1, tmpos2 - tmpos1);
		ss->str(tmp_value_string);
		ss->clear();
		ss->seekg(0, ios_base::beg);
		*ss >> tmp_value;
		tmpos1 = tmpos2 + 1;
		if(rec->compare(tmpos1, tmp_value, key) == 0){
			tmpos1 += tmp_value + 1;
			tmpos2 = rec->find("\n", tmpos1);
			if(tmpos2 != string::npos){
				tmpos1 = tmpos2 + 1;
				tmpos3 = tmpos1;
			}else{
				break;
			}
		}else{
			tmpos1 += tmp_value + 1;
			tmpos2 = rec->find("\n", tmpos1);
			if(tmpos2 != string::npos){
				tmpos1 = tmpos2 + 1;
				newrec->append(*rec, tmpos3, tmpos1 - tmpos3);
				tmpos3 = tmpos1;
			}else{
				break;
			}
		}
	}
	if(newrec->size() == 0){
		tmp_value_string.clear();
		ss->str(tmp_value_string);
		ss->clear();
		ss->seekp(0, ios_base::beg);
		*ss << idx_chunk_num;
		ss->write(",", 1);
		BDB::AddrType addrtmp = -1;
		*ss << addrtmp;
		ss->write("\n", 1);
		fwrite(ss->str().c_str(), 1, ss->str().size(), this->idx_saving_handle);
		fflush(this->idx_saving_handle);
		if(this->bdb->del(this->key_hashing_table[idx_chunk_num]) == -1){
			this->key_hashing_table[idx_chunk_num] = -1;
			return false;
		}
		this->key_hashing_table[idx_chunk_num] = -1;
	}else{
		this->bdb->update(*newrec, this->key_hashing_table[idx_chunk_num]);
	}
	return true;
}

bool BDBIDX::del_key(const char *key, size_t key_len, BDB::AddrType addr){
	
	using namespace std;

	size_t idx_chunk_num = (*BKDRHash)(key, key_len) % this->key_hashing_table_size;
	if(this->key_hashing_table[idx_chunk_num] == -1){
		return true;
	}
	auto_ptr<std::string> rec(new string);
	size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
	if(already_reading == -1){
		return false;
	}
	size_t tmpos1 = 0;
	size_t tmpos2 = 0;
	size_t tmpos3 = 0;
	size_t tmp_value = 0;
	BDB::AddrType tmp_addr = -1;
	std::auto_ptr<stringstream> ss(new stringstream(ios::in | ios::out | ios::binary));
	std::string tmp_value_string = "";
	std::auto_ptr<std::string> newrec(new std::string);
	while((tmpos2 = rec->find(",", tmpos1)) != std::string::npos){
		tmp_value_string = rec->substr(tmpos1, tmpos2 - tmpos1);
		ss->str(tmp_value_string);
		ss->clear();
		ss->seekg(0, std::ios_base::beg);
		*ss >> tmp_value;
		tmpos1 = tmpos2 + 1;
		if(rec->compare(tmpos1, tmp_value, key) == 0){
			tmpos1 += tmp_value + 1;
			tmpos2 = rec->find("\n", tmpos1);
			if(tmpos2 != std::string::npos){
				tmp_value_string = rec->substr(tmpos1, tmpos2 - tmpos1);
				ss->str(tmp_value_string);
				ss->clear();
				ss->seekg(0, std::ios_base::beg);
				*ss >> tmp_addr;
				tmpos1 = tmpos2 + 1;
				if(tmp_addr == addr){
					newrec->append(*rec, tmpos1, rec->size() - tmpos1);
					break;
				}else{
					newrec->append(*rec, tmpos3, tmpos1 - tmpos3);
				}
				tmpos3 = tmpos1;
			}else{
				break;
			}
		}else{
			tmpos1 += tmp_value + 1;
			tmpos2 = rec->find("\n", tmpos1);
			if(tmpos2 != std::string::npos){
				tmpos1 = tmpos2 + 1;
				newrec->append(*rec, tmpos3, tmpos1 - tmpos3);
				tmpos3 = tmpos1;
			}else{
				break;
			}
		}
	}
	if(newrec->size() == 0){
		tmp_value_string.clear();
		ss->str(tmp_value_string);
		ss->clear();
		ss->seekp(0, std::ios_base::beg);
		*ss << idx_chunk_num;
		ss->write(",", 1);
		BDB::AddrType addrtmp = -1;
		*ss << addrtmp;
		ss->write("\n", 1);
		fwrite(ss->str().c_str(), 1, ss->str().size(), this->idx_saving_handle);
		fflush(this->idx_saving_handle);
		if(this->bdb->del(this->key_hashing_table[idx_chunk_num]) == -1){
			this->key_hashing_table[idx_chunk_num] = -1;
			return false;
		}
		this->key_hashing_table[idx_chunk_num] = -1;
	}else{
		this->bdb->update(*newrec, this->key_hashing_table[idx_chunk_num]);
	}
	return true;
}

bool BDBIDX::is_in(const char *key, size_t key_len){
	
	using namespace std;

	auto_ptr<std::set<BDB::AddrType> > addrs(new std::set<BDB::AddrType>);
	size_t total_rec = this->get_value(key, key_len, addrs.get());
	if(total_rec > 0){
		return true;
	}
	return false;
}

size_t BDBIDX::get_value(const char *key, size_t key_len, std::set<BDB::AddrType> *addrs)
{
    assert(0 != addrs && "Null parameter");

	size_t idx_chunk_num = (*BKDRHash)(key, key_len) % key_hashing_table_size;
	if(key_hashing_table[idx_chunk_num] == -1){
		return 0;
	}
	std::auto_ptr<std::string> rec(new std::string);
	size_t already_reading = bdb->get(rec.get(), -1, key_hashing_table[idx_chunk_num], 0);
	if(already_reading == -1){
		return 0;
	}
	std::set<BDB::AddrType> *tmp_addrs = get_key_info(key, key_len, *(rec));
	*addrs = *tmp_addrs;
	delete tmp_addrs;
	return addrs->size();
}

size_t BDBIDX::get_pool(const char *key, size_t key_len, boost::unordered_multimap<std::string, BDB::AddrType> *addrs){
	size_t idx_chunk_num = (*BKDRHash)(key, key_len) % this->key_hashing_table_size;
	if(this->key_hashing_table[idx_chunk_num] == -1){
		return 0;
	}
	std::auto_ptr<std::string> rec(new std::string);
	size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
	if(already_reading == -1){
		return 0;
	}
	boost::unordered_multimap<std::string, BDB::AddrType> *tmp_addrs = this->get_key_info(*(rec));
	*addrs = *tmp_addrs;
	delete tmp_addrs;
	return addrs->size();
}

boost::unordered_multimap<std::string, BDB::AddrType>* 
BDBIDX::get_key_info(std::string &rec_content)
{

	using namespace std;

	size_t tmpos1 = 0;
	size_t tmpos2 = 0;
	size_t tmp_value = 0;
	BDB::AddrType tmp_addr = -1;
	auto_ptr<stringstream> ss(new stringstream(ios::in | ios::out | ios::binary));
	std::string key;
	string tmp_value_string = "";
	boost::unordered_multimap<std::string, BDB::AddrType> *keyinfo = new boost::unordered_multimap<std::string, BDB::AddrType>;
	while((tmpos2 = rec_content.find(",", tmpos1)) != string::npos){
		tmp_value_string = rec_content.substr(tmpos1, tmpos2 - tmpos1);
		ss->str(tmp_value_string);
		ss->clear();
		ss->seekg(0, ios_base::beg);
		*ss >> tmp_value;
		tmpos1 = tmpos2 + 1;
		key = rec_content.substr(tmpos1, tmp_value);
		tmpos1 += tmp_value + 1;
		tmpos2 = rec_content.find("\n", tmpos1);
		if(tmpos2 != string::npos){
			tmp_value_string = rec_content.substr(tmpos1, tmpos2 - tmpos1);
			ss->str(tmp_value_string);
			ss->clear();
			ss->seekg(0, ios_base::beg);
			*ss >> tmp_addr;
			keyinfo->insert(std::pair<std::string, BDB::AddrType>(key, tmp_addr));
			tmpos1 = tmpos2 + 1;
		}else{
			break;
		}
	}
	return keyinfo;
}

std::set<BDB::AddrType>* 
BDBIDX::get_key_info(const char *key, size_t key_len, std::string const &rec_content)
{
	using namespace std;
    using namespace boost::iostreams;
    using BDB::AddrType;
    using boost::tokenizer;
    using boost::escaped_list_separator;
    using boost::lexical_cast;
   
	set<AddrType> *keyinfo = new set<BDB::AddrType>;
    
    // turn rec_content into istream
    filtering_istream arr_in;
    arr_in.push(newline_filter(newline::posix));
    arr_in.push(array_source(rec_content.c_str(), rec_content.size()));

    string line;
    typedef tokenizer<escaped_list_separator<char> > tokenizerT;
    
    // assme there are 3 token for each line where are
    // key_len ',' key ',' address
    while(getline(arr_in, line)) {
        tokenizerT tok(line);
        size_t cur_key_len;
        AddrType cur_addr;
        
        tokenizerT::iterator i = tok.begin();
        
        cur_key_len = lexical_cast<size_t>(*i);
        ++i;

        if(0 != i->compare(0, cur_key_len, key, key_len))
            continue;

        if(i->size() != cur_key_len){
            // TODO  data broken
        }
        ++i;
        cur_addr = lexical_cast<AddrType>(*i);
        keyinfo->insert(cur_addr);
    }
    
    /*
	size_t tmpos1 = 0;
	size_t tmpos2 = 0;
	size_t tmp_value = 0;
	AddrType tmp_addr = -1;
	
	set<AddrType> *keyinfo = new set<BDB::AddrType>;
    //auto_ptr<stringstream> ss(new stringstream(ios::in | ios::out | ios::binary));
	//string tmp_value_string = "";
 
	while((tmpos2 = rec_content.find(",", tmpos1)) != string::npos){
		
        tmp_value_string = rec_content.substr(tmpos1, tmpos2 - tmpos1);
		ss->str(tmp_value_string);
		ss->clear();
		ss->seekg(0, ios_base::beg);
		*ss >> tmp_value;

		tmpos1 = tmpos2 + 1;

		if(rec_content.compare(tmpos1, tmp_value, key, key_len) == 0){
			tmpos1 += tmp_value + 1;
			tmpos2 = rec_content.find("\n", tmpos1);
			if(tmpos2 != string::npos){
				tmp_value_string = rec_content.substr(tmpos1, tmpos2 - tmpos1);
				ss->str(tmp_value_string);
				ss->clear();
				ss->seekg(0, ios_base::beg);
				*ss >> tmp_addr;
				keyinfo->insert(tmp_addr);
				tmpos1 = tmpos2 + 1;
			}else{
				break;
			}
		}else{
			tmpos1 += tmp_value + 1;
			tmpos2 = rec_content.find("\n", tmpos1);
			if(tmpos2 != string::npos){
				tmpos1 = tmpos2 + 1;
			}else{
				break;
			}
		}
	}
    */

	return keyinfo;
}

/*
size_t BDBIDX::BKDRHash(const char *str, size_t str_len)
{
	size_t hash = 0;
	size_t i = 0;
	while (i < str_len)
	{
		hash = ((hash << 7) + 3) + str[i];
		i++;
	}
	return hash;
}
*/
