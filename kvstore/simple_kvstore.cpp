#include "simple_kvstore.hpp"
#include <iostream>
bool SimpleKvStore::Get(const GetRequest* req, GetResponse* res) {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  if (internal_map.find(req->key) != internal_map.end()){
    res->value = internal_map[req->key];
    mutex.unlock();
    return true;
  }
  mutex.unlock();
  return false;
}

bool SimpleKvStore::Put(const PutRequest* req, PutResponse*) {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  internal_map[req->key] = req->value;
  mutex.unlock();
  return true;
}

bool SimpleKvStore::Append(const AppendRequest* req, AppendResponse*) {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  if (internal_map.find(req->key) == internal_map.end()){
    // if k/v pair doesn't exist, create new one
    internal_map[req->key] = req->value;
  } else{
    // k/v pair exists, append
    internal_map[req->key] += req->value;
  }
  mutex.unlock();
  return true;
}

bool SimpleKvStore::Delete(const DeleteRequest* req, DeleteResponse* res) {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  if (internal_map.find(req->key) != internal_map.end()){
    res->value = internal_map[req->key];
    internal_map.erase(req->key);
    mutex.unlock();
    return true;
  } else {
    mutex.unlock();
    return false;
  }
}

bool SimpleKvStore::MultiGet(const MultiGetRequest* req,
                             MultiGetResponse* res) {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  for (size_t i = 0; i < req->keys.size(); i++){
    if (internal_map.find(req->keys[i]) == internal_map.end()){
      mutex.unlock();
      return false;
    }
    res->values.push_back(internal_map[req->keys[i]]);
  }
  mutex.unlock();
  return true;
}

bool SimpleKvStore::MultiPut(const MultiPutRequest* req, MultiPutResponse*) {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  if (req->keys.size() != req->values.size()){
    mutex.unlock();
    return false;
  }
  for (size_t i = 0; i < req->keys.size(); i++){
    internal_map[req->keys[i]] = req->values[i];
  }
  mutex.unlock();
  return true;
}

std::vector<std::string> SimpleKvStore::AllKeys() {
  // TODO (Part A, Step 1 and Step 2): Implement!
  mutex.lock();
  std::vector<std::string> all_keys;
  std::map<std::string, std::string>::iterator curr = internal_map.begin();
  while (curr != internal_map.end()){
    all_keys.push_back(curr->first);
    curr++;
  }
  mutex.unlock();
  return all_keys;
}
