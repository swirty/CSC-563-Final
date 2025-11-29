from xmlrpc import server, client
import threading
import time


class Node:
  # Master, ChunkMaster, Chunk
  # the node represented by this object
  current_node = {"node_type":"Chunk", "ip":None, "port":None}
  # list of nodes which are direct children
  sub_cluster = []
  # the list of parent nodes, from nearest to furthest, ascending the tree structure.
  parent_nodes = []
  datastore = {}

  # Add a child node
  def add_child(self, child):
    # not master nor chunkmaster
    if(self.current_node["node_type"] != "Master" and self.current_node["node_type"] != "ChunkMaster"):
      return
    # adding something other than chunkmaster to master
    if(self.current_node["node_type"] == "Master" and child["node_type"] != "ChunkMaster"):
      return
    # adding something other than chunk to chunkmaster
    if(self.current_node["node_type"] == "ChunkMaster" and child["node_type"] != "Chunk"):
      return

    # guard clauses passed
    #print("check success")
    self.sub_cluster.append(child)
    #print(self.sub_cluster[0])
    return

  # Will overwrite identical keys
  # Returns false, or the next node to query.
  def add_data(self, name, data):
    match self.current_node["node_type"]:
      case "Chunk":
        self.datastore[name] = data
        return False

      case _:
        return self.sub_cluster[0]
  
  # Returns false, or the next node to query
  def get_data(self, name):
    match self.current_node["node_type"]:
      case "Chunk":
        data = None
        try:
          data = self.datastore[name]
          return data
        except:
          return NameError("Not Found")
        
      case _:
        return self.sub_cluster[0]
      
  # Returns false, or the next node to query
  def remove_data(self, name):
    match self.current_node["node_type"]:
      case "Chunk":
        try:
          self.datastore.pop(name)
          return False

        except:
          return NameError(name + " Not Found")
        
      case _:
        return self.sub_cluster[0]

  # RPC Node Functions
  # replies to heartbeat requests
  def heartbeat_reply():
    return False

  # Runs in a separate thread and manages heartbeats, queries all sub nodes and parent nodes
  def heartbeat(self):
    # sends a heartbeat request (sub-function)
    def heartbeat_send(node):
      print("heartbeat " + node)
      return

    while True:
      time.sleep(5)
      #print("beat")
      for node in self.sub_cluster:
        heartbeat_send(node)
      for node in self.parent_nodes:
        heartbeat_send(node)

  def __init__(self, node_type, sub_cluster, parent_nodes, ip, port):
    self.sub_cluster.append(sub_cluster)
    self.parent_nodes.append(parent_nodes)
    self.sub_cluster = list(filter(None, self.sub_cluster))
    self.parent_nodes = list(filter(None, self.parent_nodes))
    self.current_node = {"node_type":node_type, "ip":ip, "port":port}
    #print(self.current_node, self.sub_cluster, self.parent_nodes)

    if(self.current_node['node_type'] == "Master" or self.current_node['node_type'] == "ChunkMaster"):
      if self.sub_cluster == []:
        print("start subcluster servers")

    # begins the heartbeat thread
    heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
    heartbeat_thread.start()

    # This node acts as server
    srv = server.SimpleXMLRPCServer((self.current_node["ip"], self.current_node['port']))
    srv.register_function(self.add_child, "add_child")
    srv.register_function(self.add_data, "add_data")
    srv.register_function(self.get_data, "get_data")
    srv.register_function(self.remove_data, "remove_data")
    srv.register_function(self.heartbeat_reply, "heartbeat")
    
    srv.serve_forever()


################
## Cold Start ##
################
master = Node("Master", None, None, "localhost", 9000)