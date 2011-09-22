#include "rec_db.hpp"

#include "BDBIDX.h"
#include "bdb/bdb.hpp"
#include <set>

namespace rec_db {

db::db(BDB::Config const& conf)
{
    store_ = new BDB::BehaviorDB(conf);
    idx_ = new BDBIDX(conf.root_dir);
}

db::~db()
{
    delete idx_;
    delete store_;
}

bool 
db::put(std::string const& key, std::string const& data)
{
    BDB::AddrType addr;
    if(-1 == (addr = store_->put(data)))
        return false;
    if(false == idx_->put_key(key, addr))
        return false;
    return true;
}

bool
db::get(std::string *output, std::string key)
{
    std::set<BDB::AddrType> addrs;
    if(1 != idx_->get_value(key, &addrs))
        return false;
    output->clear();
    store_->get(output, -1, *addrs.begin());
    return true;
}

bool
db::del(std::string const& key)
{
        
    std::set<BDB::AddrType> addrs;
    if(1 != idx_->get_value(key, &addrs))
        return false;
    if(0 != store_->del(*addrs.begin()))
        return false;
    return idx_->del_key(key, *addrs.begin());
}

bool
db::update(std::string const& key, std::string const& data)
{
    std::set<BDB::AddrType> addrs;
    if(1 != idx_->get_value(key, &addrs))
        return false;
    if(-1 == store_->update(data, *addrs.begin()))
        return false;
    return true;
}


} // namespace rec_db

