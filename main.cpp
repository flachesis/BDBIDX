/* 
 * File:   main.cpp
 * Author: flachesis
 *
 * Created on 2011年7月3日, 下午 5:31
 */

#include "BDBIDX.h"
#include <iostream>

using namespace std;

/*
 * 
 */
int main(int argc, char** argv) {
    BDBIDX idx("tmp");
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
//    idx.del_key("test", 4);
    idx.del_key("test", 4, 1);
    idx.del_key("test", 4, 2);
    idx.del_key("test", 4, 3);
    idx.del_key("test2", 5);
    std::auto_ptr<std::set<BDB::AddrType> > addrinfo(idx.get_value("test", 4));
    if(addrinfo.get() == NULL){
        std::cerr << "empty" << std::endl;
        return 0;
    }else if(addrinfo->size() == 0){
        std::cerr << "size zero" << std::endl;
        return 0;
    }
    std::cerr << "size: " << addrinfo->size() << std::endl;
    for(std::set<BDB::AddrType>::iterator it = addrinfo->begin(); it != addrinfo->end(); it++){
        std::cerr << "addr" << *it << std::endl;
    }
    return 0;
}

