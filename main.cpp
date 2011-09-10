/* 
 * File:   main.cpp
 * Author: flachesis
 *
 * Created on 2011年7月3日, 下午 5:31
 */

#include "BDBIDX.h"
#include <iostream>
#include <memory>
using namespace std;

/*
 * 
 */
struct test_pool_hash : hashFunc{
	size_t
	operator()(char const *key, size_t len) const
	{
		return key[0];
	}
	~test_pool_hash(){};
};
int main(int argc, char** argv) {
    test_pool_hash tph;
    BDBIDX idx("tmp", 50000000, &tph);
    std::cerr << idx.put_key("test", 4, 0) << std::endl;
    std::cerr << idx.put_key("test", 4, 1) << std::endl;
    std::cerr << idx.put_key("test", 4, 2) << std::endl;
    std::cerr << idx.put_key("test", 4, 3) << std::endl;
    std::cerr << idx.put_key("test", 4, 4) << std::endl;
    std::cerr << idx.put_key("test2", 5, 5) << std::endl;
    std::cerr << idx.put_key("test2", 5, 6) << std::endl;
    std::cerr << idx.put_key("test2", 5, 7) << std::endl;
    std::cerr << idx.put_key("test2", 5, 8) << std::endl;
    std::cerr << idx.put_key("test2", 5, 9) << std::endl;
    std::cerr << idx.put_key("test2", 5, 8) << std::endl;
    std::cerr << idx.put_key("xest", 4, 0) << std::endl;
    std::auto_ptr<boost::unordered_multimap<std::string, BDB::AddrType> > poolinfo(new boost::unordered_multimap<std::string, BDB::AddrType>);
    std::string my_key("x");
    idx.get_pool(my_key, poolinfo.get());
    for(boost::unordered_multimap<std::string, BDB::AddrType>::iterator it = poolinfo->begin(); it != poolinfo->end(); it++){
	    std::cout << "key: " << it->first << "\tvalue: " << it->second << std::endl;
    }
//    idx.del_key("test", 4);
//    idx.del_key("test", 4, 1);
//    idx.del_key("test", 4, 2);
//    idx.del_key("test", 4, 3);
//    idx.del_key("test2", 5);
//    std::auto_ptr<std::set<BDB::AddrType> > addrinfo(new std::set<BDB::AddrType>);
//    size_t addrinfo_size = idx.get_value("test", 4, addrinfo.get());
//    if(addrinfo.get() == 0){
//        std::cerr << "empty" << std::endl;
//        return 0;
//    }else if(addrinfo->size() == 0){
//        std::cerr << "size zero" << std::endl;
//        return 0;
//    }
//    std::cerr << "size: " << addrinfo->size() << std::endl;
//    for(std::set<BDB::AddrType>::iterator it = addrinfo->begin(); it != addrinfo->end(); it++){
//        std::cerr << "addr" << *it << std::endl;
//    }
    return 0;
}

