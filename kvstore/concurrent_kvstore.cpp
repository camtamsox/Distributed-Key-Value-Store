#include "concurrent_kvstore.hpp"

#include <mutex>
#include <optional>
#include <set>

bool ConcurrentKvStore::Get(const GetRequest* req, GetResponse* res) {
  // TODO (Part A, Step 3 and Step 4): Implement!
  store.mutexes[store.bucket(req->key)].lock_shared();
  std::optional<DbItem> get_ret = store.getIfExists(store.bucket(req->key),req->key);
  if (get_ret != std::nullopt){
    res->value = get_ret->value;
    store.mutexes[store.bucket(req->key)].unlock_shared();
    return true;
  }
  store.mutexes[store.bucket(req->key)].unlock_shared();
  return false;
}

bool ConcurrentKvStore::Put(const PutRequest* req, PutResponse*) {
  // TODO (Part A, Step 3 and Step 4): Implement!
  store.mutexes[store.bucket(req->key)].lock();
  store.insertItem(store.bucket(req->key),req->key,req->value);
  store.mutexes[store.bucket(req->key)].unlock();
  return true;
}

bool ConcurrentKvStore::Append(const AppendRequest* req, AppendResponse*) {
  // TODO (Part A, Step 3 and Step 4): Implement!
  store.mutexes[store.bucket(req->key)].lock();
  std::optional<DbItem> get_ret = store.getIfExists(store.bucket(req->key),req->key);
  if (get_ret != std::nullopt){
    store.insertItem(store.bucket(req->key),req->key,get_ret->value+req->value);
  } else {
    store.insertItem(store.bucket(req->key),req->key,req->value);
  }
  store.mutexes[store.bucket(req->key)].unlock();
  return true;
}

bool ConcurrentKvStore::Delete(const DeleteRequest* req, DeleteResponse* res) {
  // TODO (Part A, Step 3 and Step 4): Implement!
  store.mutexes[store.bucket(req->key)].lock();
  std::optional<DbItem> get_ret = store.getIfExists(store.bucket(req->key),req->key);
  if (get_ret != std::nullopt){
    res->value = get_ret->value;
    bool ret = store.removeItem(store.bucket(req->key),req->key);
    store.mutexes[store.bucket(req->key)].unlock();
    return ret;
  } else {
    store.mutexes[store.bucket(req->key)].unlock();
    return false;
  }
}

bool ConcurrentKvStore::MultiGet(const MultiGetRequest* req,
                                 MultiGetResponse* res) {
  // lock mutexes
  std::set<size_t> bucket_indices;
  for (size_t i = 0; i < req->keys.size(); i++){
    bucket_indices.insert(store.bucket(req->keys[i]));
  }
  for (size_t bucket : bucket_indices){
    store.mutexes[bucket].lock_shared();
  }
  // do multiget
  for (size_t i = 0; i < req->keys.size();i++){
    std::optional<DbItem> get_ret = store.getIfExists(store.bucket(req->keys[i]),req->keys[i]);
    if (get_ret == std::nullopt){
      // unlock mutexes
      for (size_t bucket : bucket_indices){
        store.mutexes[bucket].unlock_shared();
      }
      return false;
    }
    res->values.push_back(get_ret->value);
  }
  // unlock mutexes
  for (size_t bucket : bucket_indices){
    store.mutexes[bucket].unlock_shared();
  }
  return true;
}

bool ConcurrentKvStore::MultiPut(const MultiPutRequest* req,
                                 MultiPutResponse*) {
  // TODO (Part A, Step 3 and Step 4): Implement!
  if (req->keys.size() != req->values.size()){
    return false;
  }
  // lock mutexes
  std::set<size_t> bucket_indices;
  for (size_t i = 0; i < req->keys.size(); i++){
    bucket_indices.insert(store.bucket(req->keys[i]));
  }
  for (size_t bucket : bucket_indices){
    store.mutexes[bucket].lock();
  }
  // do multiput
  for (size_t i = 0; i < req->keys.size(); i++){
    store.insertItem(store.bucket(req->keys[i]),req->keys[i],req->values[i]);
  }
  // unlock mutexes
  for (size_t bucket : bucket_indices){
    store.mutexes[bucket].unlock();
  }
  return true;
}

std::vector<std::string> ConcurrentKvStore::AllKeys() {
  // lock mutexes
  for (size_t i = 0; i < store.mutexes.size(); i++){
    store.mutexes[i].lock_shared();
  }
  // get keys
  std::vector<std::string> all_keys;
  for (size_t i = 0; i < store.BUCKET_COUNT; i++){
    std::list<DbItem>::iterator it = store.buckets[i].begin();
    while (it != store.buckets[i].end()){
      all_keys.push_back(it->key);
      it++;
    }
  }
  // unlock mutexes
  for (size_t i = 0; i < store.mutexes.size(); i++){
    store.mutexes[i].unlock_shared();
  }
  return all_keys;
}
