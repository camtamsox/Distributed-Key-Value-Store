#include "shardkv_client.hpp"

std::optional<std::string> ShardKvClient::Get(const std::string& key) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return std::nullopt;

  // find responsible server in config
  std::optional<std::string> server = config->get_server(key);
  if (!server) return std::nullopt;

  return SimpleClient{*server}.Get(key);
}

bool ShardKvClient::Put(const std::string& key, const std::string& value) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return false;

  // find responsible server in config, then make Put request
  std::optional<std::string> server = config->get_server(key);
  if (!server) return false;
  return SimpleClient{*server}.Put(key, value);
}

bool ShardKvClient::Append(const std::string& key, const std::string& value) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return false;

  // find responsible server in config, then make Append request
  std::optional<std::string> server = config->get_server(key);
  if (!server) return false;
  return SimpleClient{*server}.Append(key, value);
}

std::optional<std::string> ShardKvClient::Delete(const std::string& key) {
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return std::nullopt;

  // find responsible server in config, then make Delete request
  std::optional<std::string> server = config->get_server(key);
  if (!server) return std::nullopt;
  return SimpleClient{*server}.Delete(key);
}

std::optional<std::vector<std::string>> ShardKvClient::MultiGet(
    const std::vector<std::string>& keys) {
  std::vector<std::string> values;
  values.resize(keys.size());
  // TODO (Part B, Step 3): Implement!
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return std::nullopt;

  // find responsible servers in config
  std::map<std::string, std::pair<std::vector<int>, std::vector<std::string>>> serverGets; // server -> (key indices, keys to get)
  for (size_t i = 0; i < keys.size();i++){
    std::string key = keys[i];
    // check if key's server is in serverGets
    std::optional<std::string> server = config->get_server(key); // first get the key's server
    if (!server) {// key doesn't have a server
      return std::nullopt;
    } 

    if (serverGets.find(server.value()) == serverGets.end()) {
      // server not in serverGets so add it
      serverGets[server.value()] = std::pair<std::vector<int>, std::vector<std::string>>();
    }
    serverGets[server.value()].first.push_back(i);
    serverGets[server.value()].second.push_back(key);
  }

  // do MultiGet for each server
  for (std::map<std::string, std::pair<std::vector<int>, std::vector<std::string>>>::iterator it = serverGets.begin();it!=serverGets.end();it++){
    std::string server_name = it->first;
    std::vector<int> key_indices = it->second.first;
    std::vector<std::string> keys_to_get = it->second.second;
    std::optional<std::vector<std::string>> response = SimpleClient{it->first}.MultiGet(keys_to_get);
    // add to values vector
    if (!response){
      return std::nullopt;
    }
    for (size_t i = 0; i < response.value().size(); i++){
      std::string res = response.value()[i];
      values[key_indices[i]] = res;
    }
  }
  return values;
}

bool ShardKvClient::MultiPut(const std::vector<std::string>& keys,
                             const std::vector<std::string>& values) {
  // TODO (Part B, Step 3): Implement!
  // Query shardcontroller for config
  auto config = this->Query();
  if (!config) return false;

  // find responsible servers in config
  std::map<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>> serverPuts; // server -> (keys to put, values to put)
  for (size_t i = 0; i < keys.size();i++){
    std::string key = keys[i];
    std::string value = values[i];
    // check if key's server is in serverGets
    std::optional<std::string> server = config->get_server(key); // first get the key's server
    if (!server) {// key doesn't have a server
      return false;
    } 

    if (serverPuts.find(server.value()) == serverPuts.end()) {
      // server not in serverGets so add it
      serverPuts[server.value()] = std::pair<std::vector<std::string>, std::vector<std::string>>();
    }
    serverPuts[server.value()].first.push_back(key);
    serverPuts[server.value()].second.push_back(value);
  }

  // do MultiPut for each server
  for (std::map<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>>::iterator it = serverPuts.begin();it!=serverPuts.end();it++){
    std::string server_name = it->first;
    std::vector<std::string> keys_to_put = it->second.first;
    std::vector<std::string> values_to_put = it->second.second;
    bool response = SimpleClient{it->first}.MultiPut(keys_to_put,values_to_put);
    // add to values vector
    if (!response){
      return false;
    }
  }
  return true;
}

// Shardcontroller functions
std::optional<ShardControllerConfig> ShardKvClient::Query() {
  QueryRequest req;
  if (!this->shardcontroller_conn->send_request(req)) return std::nullopt;

  std::optional<Response> res = this->shardcontroller_conn->recv_response();
  if (!res) return std::nullopt;
  if (auto* query_res = std::get_if<QueryResponse>(&*res)) {
    return query_res->config;
  }

  return std::nullopt;
}

bool ShardKvClient::Move(const std::string& dest_server,
                         const std::vector<Shard>& shards) {
  MoveRequest req{dest_server, shards};
  if (!this->shardcontroller_conn->send_request(req)) return false;

  std::optional<Response> res = this->shardcontroller_conn->recv_response();
  if (!res) return false;
  if (auto* move_res = std::get_if<MoveResponse>(&*res)) {
    return true;
  }

  return false;
}
