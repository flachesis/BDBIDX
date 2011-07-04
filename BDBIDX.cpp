/* 
 * File:   BDBIDX.cpp
 * Author: flachesis
 * 
 * Created on 2011年7月3日, 下午 5:37
 */


#include <iostream>

#include "BDBIDX.h"

BDBIDX::BDBIDX(const char *idx_dir) {
    this->init_bdbidx(idx_dir, 50000000);
}

BDBIDX::BDBIDX(const char *idx_dir, size_t key_hashing_table_size){
    this->init_bdbidx(idx_dir, key_hashing_table_size);
}

BDBIDX::BDBIDX(const BDBIDX& orig) {
}

BDBIDX::~BDBIDX() {
    delete [] this->key_hashing_table;
    delete this->bdb;
    fclose(this->idx_saving_handle);
}

void BDBIDX::init_bdbidx(const char *idx_dir, size_t key_hashing_table_size){
    this->idx_dir = idx_dir;
    if(*(this->idx_dir.end() - 1) != '/'){
        this->idx_dir += "/";
    }
    std::string idx_saving_file = this->idx_dir + "addr_idx.log";
    this->idx_saving_handle = fopen(idx_saving_file.c_str(), "a+b");
    BDB::Config bdb_config;
    bdb_config.root_dir = this->idx_dir.c_str();
    bdb_config.min_size = 32;
    this->bdb = new BDB::BehaviorDB(bdb_config);
    this->key_hashing_table_size = key_hashing_table_size;
    this->key_hashing_table = new BDB::AddrType[this->key_hashing_table_size];
    memset(this->key_hashing_table, -1, this->key_hashing_table_size);
    if(this->idx_saving_handle != NULL){
        size_t file_size = ftell(this->idx_saving_handle);
        char *file_content = new char[file_size + 1];
        fseek(this->idx_saving_handle, 0, SEEK_SET);
        fread(file_content, 1, file_size, this->idx_saving_handle);
        file_content[file_size] = '\0';
        char *pch = strtok(file_content, "\n");
        size_t tmp_value;
        BDB::AddrType tmp_addr = -1;
        while(pch != NULL){
            sscanf(pch, "%u,%u", &tmp_value, &tmp_addr);
            this->key_hashing_table[tmp_value] = tmp_addr;
            pch = strtok(NULL, "\n");
        }
    }
}

bool BDBIDX::put_key(const char *key, size_t key_len, BDB::AddrType addr){
    size_t idx_chunk_num = this->BKDRHash(key, key_len) / this->key_hashing_table_size;
    if(this->key_hashing_table[idx_chunk_num] == -1){
        std::auto_ptr<std::stringstream> ss(new std::stringstream(std::stringstream::in | std::stringstream::out | std::stringstream::binary));
        *ss << key_len;
        ss->write(",", 1);
        ss->write(key, key_len);
        ss->write(",", 1);
        *ss << addr;
        ss->write("\n", 1);
        this->key_hashing_table[idx_chunk_num] = this->bdb->put(ss->str());
        if(this->key_hashing_table[idx_chunk_num] == -1){
            return false;
        }
        std::string empty_string;
        empty_string.clear();
        ss->str(empty_string);
        ss->clear();
        ss->seekp(0, std::ios_base::beg);
        *ss << idx_chunk_num;
        ss->write(",", 1);
        *ss << this->key_hashing_table[idx_chunk_num];
        ss->write("\n", 1);
        fwrite(ss->str().c_str(), 1, ss->str().size(), this->idx_saving_handle);
        fflush(this->idx_saving_handle);
        return true;
    }
    std::auto_ptr<std::string> rec(new std::string);
    size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
    if(already_reading == -1){
        return false;
    }
    std::auto_ptr<std::set<BDB::AddrType> > keyinfo(this->get_key_info(key, key_len, *(rec)));
    if(keyinfo->find(addr) != keyinfo->end()){
        return true;
    }
    std::auto_ptr<std::stringstream> ss(new std::stringstream(std::stringstream::in | std::stringstream::out | std::stringstream::binary));
    *ss << key_len;
    ss->write(",", 1);
    ss->write(key, key_len);
    ss->write(",", 1);
    *ss << addr;
    ss->write("\n", 1);
    this->bdb->put(ss->str(), this->key_hashing_table[idx_chunk_num], -1);
    return true;
}

bool BDBIDX::del_key(const char *key, size_t key_len){
    size_t idx_chunk_num = this->BKDRHash(key, key_len) / this->key_hashing_table_size;
    if(this->key_hashing_table[idx_chunk_num] == -1){
        return true;
    }
    std::auto_ptr<std::string> rec(new std::string);
    size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
    if(already_reading == -1){
        return false;
    }
    size_t tmpos1 = 0;
    size_t tmpos2 = 0;
    size_t tmpos3 = 0;
    size_t tmp_value = 0;
    std::auto_ptr<std::stringstream> ss(new std::stringstream(std::stringstream::in | std::stringstream::out | std::stringstream::binary));
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
                tmpos1 = tmpos2 + 1;
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

bool BDBIDX::del_key(const char *key, size_t key_len, BDB::AddrType addr){
    size_t idx_chunk_num = this->BKDRHash(key, key_len) / this->key_hashing_table_size;
    if(this->key_hashing_table[idx_chunk_num] == -1){
        return true;
    }
    std::auto_ptr<std::string> rec(new std::string);
    size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
    if(already_reading == -1){
        return false;
    }
    size_t tmpos1 = 0;
    size_t tmpos2 = 0;
    size_t tmpos3 = 0;
    size_t tmp_value = 0;
    BDB::AddrType tmp_addr = -1;
    std::auto_ptr<std::stringstream> ss(new std::stringstream(std::stringstream::in | std::stringstream::out | std::stringstream::binary));
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

std::set<BDB::AddrType>* BDBIDX::get_addrs(const char *key, size_t key_len){
    size_t idx_chunk_num = this->BKDRHash(key, key_len) / this->key_hashing_table_size;
    if(this->key_hashing_table[idx_chunk_num] == -1){
        return NULL;
    }
    std::auto_ptr<std::string> rec(new std::string);
    size_t already_reading = this->bdb->get(rec.get(), -1, this->key_hashing_table[idx_chunk_num], 0);
    if(already_reading == -1){
        return NULL;
    }
    return this->get_key_info(key, key_len, *(rec));
}

std::set<BDB::AddrType>* BDBIDX::get_key_info(const char *key, size_t key_len, std::string &rec_content){
    size_t tmpos1 = 0;
    size_t tmpos2 = 0;
    size_t tmp_value = 0;
    BDB::AddrType tmp_addr = -1;
    std::auto_ptr<std::stringstream> ss(new std::stringstream(std::stringstream::in | std::stringstream::out | std::stringstream::binary));
    std::string tmp_value_string = "";
    std::set<BDB::AddrType> *keyinfo = new std::set<BDB::AddrType>;
    while((tmpos2 = rec_content.find(",", tmpos1)) != std::string::npos){
        tmp_value_string = rec_content.substr(tmpos1, tmpos2 - tmpos1);
        ss->str(tmp_value_string);
        ss->clear();
        ss->seekg(0, std::ios_base::beg);
        *ss >> tmp_value;
        tmpos1 = tmpos2 + 1;
        if(rec_content.compare(tmpos1, tmp_value, key, key_len) == 0){
            tmpos1 += tmp_value + 1;
            tmpos2 = rec_content.find("\n", tmpos1);
            if(tmpos2 != std::string::npos){
                tmp_value_string = rec_content.substr(tmpos1, tmpos2 - tmpos1);
                ss->str(tmp_value_string);
                ss->clear();
                ss->seekg(0, std::ios_base::beg);
                *ss >> tmp_addr;
                keyinfo->insert(tmp_addr);
                tmpos1 = tmpos2 + 1;
            }else{
                break;
            }
        }else{
            tmpos1 += tmp_value + 1;
            tmpos2 = rec_content.find("\n", tmpos1);
            if(tmpos2 != std::string::npos){
                tmpos1 = tmpos2 + 1;
            }else{
                break;
            }
        }
    }
    return keyinfo;
}

size_t BDBIDX::BKDRHash(const char *str, size_t str_len){
    size_t hash = 0;
    size_t i = 0;
    while (i < str_len)
    {
        hash = ((hash << 7) + 3) + str[i];
        i++;
    }
    return hash;
}

