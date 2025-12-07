from xmlrpc import server, client
import threading
import time
import socket


class Node:
  master_node = None
  rebuilding = False

  # returns info about the parameter node, stripping recursive dicts for serialization
  def node_info(node):
    return {"node_type":node["node_type"], "ip":node["ip"], "port":node["port"]}

  # Add a child node
  def add_child(self, child):
    # not master
    if(self.current_node["node_type"] != "Master"):
      return
    # adding something other than chunk to master
    if(self.current_node["node_type"] == "Master" and child["node_type"] != "Chunk"):
      return
    # adding something other than chunk to chunk
    if(self.current_node["node_type"] == "Chunk" and child["node_type"] != "Chunk"):
      return

    # guard clauses passed
    #print("check success")
    self.sub_cluster.append(child)
    #print(self.sub_cluster[0])
    return

  # Add a child node
  def add_parent(self, parent):
    self.current_node["node_type"] = "Chunk"
    #print("check success")
    self.parent_nodes.append(parent)
    #print(self.sub_cluster[0])
    return

  # Will overwrite identical keys
  # Returns false, or the next node to query.
  def add_data(self, name, data):
    match self.current_node["node_type"]:
      case "Chunk":
        self.datastore[name] = data
        if self.sub_cluster != []:
          for node in self.sub_cluster:
            replication_rq = client.ServerProxy(f"http://{node["ip"]}:{node["port"]}")
            replication_rq.add_data(name, data)
        return False

      case "Master":
        cluster_index = hash(name) % len(self.sub_cluster)
        self.datastore[name] = cluster_index
        return Node.node_info(self.sub_cluster[cluster_index])
  
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
        
      case "Master":
        return Node.node_info(self.sub_cluster[hash(name) % len(self.sub_cluster)])
      
  # Returns false, or the next node to query
  def remove_data(self, name):
    match self.current_node["node_type"]:
      case "Chunk":
        try:
          self.datastore.pop(name)
          return False

        except:
          return NameError(name + " Not Found")
        
      case "Master":
        return Node.node_info(self.sub_cluster[hash(name) % len(self.sub_cluster)])

  # replies to heartbeat requests
  def heartbeat_reply(self):
    return True
  
  # Runs in a separate thread and manages heartbeats, queries all sub nodes and parent nodes
  def heartbeat(self):
    # sends a heartbeat request (sub-function)
    def heartbeat_send(node, cluster_type):
      #return
      print(self.current_node["port"], "Sending heartbeat to:", node["port"], "full details:", node)
      try:
        rq = client.ServerProxy(f"http://{node["ip"]}:{node["port"]}")
        socket.setdefaulttimeout(15)
        if not rq.heartbeat_reply():
          print("PROBLEM STATE HEARTBEAT ERROR!!")
          raise Exception("Heartbeat reply was false.")
        else:
          return
      except Exception as e:
        print(f"HEARTBEAT FAILED: Node at {node["ip"]}:{node["port"]} is dead. Error: {e}")
        Node.rebuilding = True
        match cluster_type:
          case 'sub_cluster':
            # Node was in the sub_cluster of the current node, must be at the bottom of the tree
            Node("Chunk", [], [self.current_node, Node.master_node], node["ip"], node["port"])
          
          case 'parent_nodes':
            # Node was in the parent_nodes of the current node, must be in the middle of the tree
            #print(Node.master_node)
            Node("Chunk", [self.current_node], [Node.master_node], node["ip"], node["port"])

    while not self.stop_event.is_set():
      time.sleep(5)
      #print("beat")
      for node in self.sub_cluster:
        #print(node)
        if not Node.rebuilding:
          heartbeat_send(node, 'sub_cluster')
      else:
        print(self.current_node["port"], "has no sub_cluster nodes to heartbeat.")
      if self.parent_nodes != [] and not Node.rebuilding:
        heartbeat_send(self.parent_nodes[0], 'parent_nodes')
      else:
        print(self.current_node["port"], "has no parent_nodes to heartbeat. Or is rebuilding.")

  # kills the node
  def discard(self):
    def shutdown_node():
      print("shutting down node at", self.current_node['ip'], ":", self.current_node['port'])
      self.stop_event.set()
      self.srv.shutdown()
      self.server_thread.join(timeout=5)
    
    # starting shutdown thread
    t = threading.Thread(target=shutdown_node, daemon=False)
    t.start()
    t.join(timeout=10)
    print("node at", self.current_node['ip'], ":", self.current_node['port'], "shut down successfully.")

  # Cluster size defined by cluster_size * replication_factor + 1. +1 for master.
  def __init__(self, node_type, sub_cluster, parent_nodes, ip, port, cluster_size=3, replication_factor=3):
    self.current_node = {"node_type":"Chunk", "ip":None, "port":None}
    self.sub_cluster = []
    self.parent_nodes = []
    self.datastore = {}
    
    self.sub_cluster += sub_cluster
    self.parent_nodes += parent_nodes
    self.sub_cluster = list(filter(None, self.sub_cluster))
    self.parent_nodes = list(filter(None, self.parent_nodes))
    self.current_node = {"node_type":node_type, "ip":ip, "port":port}
    #print(self.current_node, self.sub_cluster, self.parent_nodes)

    # This node acts as server
    self.srv = server.SimpleXMLRPCServer((self.current_node["ip"], self.current_node['port']), allow_none=True, logRequests=False)
    self.srv.register_function(self.add_child, "add_child")
    self.srv.register_function(self.add_parent, "add_parent")
    self.srv.register_function(self.add_data, "add_data")
    self.srv.register_function(self.get_data, "get_data")
    self.srv.register_function(self.remove_data, "remove_data")
    self.srv.register_function(self.heartbeat_reply, "heartbeat_reply")
    self.srv.register_function(self.discard, "discard")
    
    # Serve RPC calls in a daemon thread
    self.server_thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
    self.server_thread.start()

    if(self.current_node['node_type'] == "Master" and self.sub_cluster == []):
      Node.master_node = self.current_node
      print("Starting subcluster servers")
      for i in range(cluster_size):
        n = Node("Chunk", [], [self.current_node], "localhost", port + i*replication_factor + 1)
        self.sub_cluster.append(n.current_node)

    if(self.current_node['node_type'] == "Chunk" and self.parent_nodes[0]["node_type"] == "Master"):
      print("Starting subcluster chunk servers")
      for i in range(replication_factor - 1):
        n = Node("Chunk", [], [] + [self.current_node] + self.parent_nodes, "localhost", port + i + 1)
        #print(n.current_node, n.parent_nodes)
        self.sub_cluster.append(n.current_node)

    self.stop_event = threading.Event()

    # begins the heartbeat thread
    self.heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
    self.heartbeat_thread.start()

    Node.rebuilding = False

    # if self.current_node['node_type'] == "Master":
    #   while True:
    #     time.sleep(1)


################
## Cold Start ##
################
master_thread = threading.Thread(target=lambda: Node(
  node_type="Master", 
  sub_cluster=[], 
  parent_nodes=[], 
  ip="localhost", 
  port=9000
), daemon=True)
master_thread.start()

print("Waiting for cluster to initialize...")
time.sleep(2)
print("Cluster initialized.")

master = client.ServerProxy("http://localhost:9000")
print("Adding data to cluster via master node...")
ret = master.add_data("example_key", "example_value")
print ("Master returned:", ret, "adding to node...")
if ret["node_type"] == "Chunk":
    rq = client.ServerProxy(f"http://{ret["ip"]}:{ret["port"]}")
    rq.add_data("example_key", "example_value")
    print("Data added.")

print("Requsting data from cluster via master node...")
ret = master.get_data("example_key")
print("Retrieving data from chunk node", ret, "directly...")
if ret["node_type"] == "Chunk":
    rq = client.ServerProxy(f"http://{ret["ip"]}:{ret["port"]}")
    data = rq.get_data("example_key")
    print("Data retrieved: ", data)

# kill a node to test heartbeat recovery
print("Killing a chunk node to test heartbeat recovery...")
if ret["node_type"] == "Chunk":
    rq = client.ServerProxy(f"http://{ret["ip"]}:{ret["port"]}")
    rq.discard()
    print("Node killed.")

# keep main thread alive
while True:
    time.sleep(1)