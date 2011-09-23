#ifndef _REC_DB_HPP
#define _REC_DB_HPP

#include <string>

// forward decls
namespace BDB{ 
    struct BehaviorDB;
    struct Config;
}
class BDBIDX;

namespace rec_db {

/** A record DB wraps BDBIDX and BehaviorDB
 */
class db
{
public:
   
    /** @brief Constructor
     *  @param conf Configuration
     *  @see BehaviorDB::Config
     */
    db(BDB::Config const& conf);
    
    ~db();
    
    /// put data to db with an unique key
    bool 
    put(std::string const& key, std::string const& data);
    
    /// get data according to a key
    bool
    get(std::string *output, std::string key);

    /// del data according to a key
    bool
    del(std::string const& key);

    /// update(replace) data according to a key
    bool
    update(std::string const& key, std::string const& data);

private:

    // non-copyable
    db(db const &cp);
    db& operator=(db const &db);

    BDB::BehaviorDB *store_;
    BDBIDX *idx_;
};

} // namespace rec_db

#endif // header guard
