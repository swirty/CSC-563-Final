from xmlrpc import server, client
import threading
import time
import socket
import keyboard
import random

class Node:
  full_cluster = []
  rebuilding = False

  #returns sibling nodes, including self
  def get_siblings(self):
    siblings = []
    if self.node_id == 0: #is it the master node
      return Node.full_cluster[0]
    if self.node_id % self.replication_factor == 1: #is it in the middle layer of the tree
      for i in range(self.cluster_size):
        siblings.append(Node.full_cluster[(self.node_id + i*self.replication_factor)%(self.cluster_size*self.replication_factor)])
    else: #it must be at the bottom layer of the tree
      offset = 0
      for i in range(self.replication_factor - 1):
        # adjust offset if at the end of a replication group
        if (self.node_id + i) % self.replication_factor == 1:
          offset = -self.replication_factor + 1
        siblings.append(Node.full_cluster[self.node_id + i + offset])
    return siblings

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
        cluster_index = hash(name) % self.cluster_size
        cluster_index *= self.replication_factor
        cluster_index += 1
        self.datastore[name] = cluster_index
        return Node.node_info(Node.full_cluster[cluster_index])
  
  # Returns false, or the next node to query
  def get_data(self, name):
    match self.current_node["node_type"]:
      case "Chunk":
        data = None
        try:
          if name == None:
            return self.datastore
          data = self.datastore[name]
          return data
        except:
          return NameError("Not Found")
        
      case "Master":
        cluster_index = hash(name) % self.cluster_size
        cluster_index =  cluster_index * self.replication_factor + 1
        #print("the middle cluster chunkserver is", cluster_index)
        cluster_index += random.randint(0, self.replication_factor - 1)
        #print("a random cluster chunkserver in that subcluster is", cluster_index)
        return Node.node_info(Node.full_cluster[cluster_index])
      
  # Returns false, or the next node to query
  def remove_data(self, name):
    match self.current_node["node_type"]:
      case "Chunk":
        try:
          print(self.datastore)
          del self.datastore[name]
          print(self.datastore)
          return False

        except:
          return NameError(name + " Not Found")
        
      case "Master":
        try:
          del self.datastore[name]
          return Node.node_info(self.sub_cluster[hash(name) % len(self.sub_cluster)])
        except:
          return NameError(name + " Not Found")
        
  
  # heals the cluster after a dead node is detected, relationship is 'sub_cluster' or 'parent_nodes' depending on where the dead node was located
  # relationship is used to determine how to heal the cluster, either by adding a new bottom layer node or a middle layer node
  def heal_cluster(self, dead_node, relationship):
    if Node.rebuilding:
      return
    Node.rebuilding = True
    print("HEALING CLUSTER INITIATED BY NODE AT PORT:", self.current_node["port"])
    # healing process
    match relationship:
      case 'sub_cluster':
        # Dead Node was in the sub_cluster of the current node, dead node must be at the bottom of the tree, and pass in current node datastore for rebuild
        Node("Chunk", [], [self.current_node, Node.full_cluster[0]], dead_node["ip"], dead_node["port"], node_id=dead_node["node_id"], cluster_size=self.cluster_size, replication_factor=self.replication_factor, rebuild=True, datastore=self.datastore)
      
      case 'parent_nodes':
        # Dead Node was in the parent_nodes of the current node, dead node must be in the middle of the tree
        #print(Node.full_cluster[0])
        Node("Chunk", self.get_siblings(), [Node.full_cluster[0]], dead_node["ip"], dead_node["port"], node_id=dead_node["node_id"], cluster_size=self.cluster_size, replication_factor=self.replication_factor, rebuild=True, datastore=self.datastore)

  # replies to heartbeat requests
  def heartbeat_reply(self):
    print("<-rp-", self.current_node["port"])
    return True

  # Runs in a separate thread and sends heartbeats, queries all sub nodes and parent nodes
  def heartbeat(self):
    # sends a heartbeat request (sub-function)
    def heartbeat_send(node, cluster_type):
      #return
      print(self.current_node["port"], "-hb->", node["port"])
      try:
        rq = client.ServerProxy(f"http://{node["ip"]}:{node["port"]}")
        socket.setdefaulttimeout(10)
        if not Node.rebuilding and not rq.heartbeat_reply():
          print("PROBLEM STATE HEARTBEAT ERROR!!")
          raise Exception("Heartbeat reply was false.")
        else:
          return
      except Exception as e:
        if not Node.rebuilding:
          print(f"HEARTBEAT FAILED: Node at {node["ip"]}:{node["port"]} is dead. Error: {e}")
          self.heal_cluster(dead_node=node, relationship=cluster_type)

    while not self.stop_event.is_set():
      while Node.rebuilding:
        time.sleep(5)
      time.sleep(5)
      #print("beat")
      for node in self.sub_cluster:
        #print(node)
        if self.stop_event.is_set():
          break
        if not Node.rebuilding:
          heartbeat_send(node, 'sub_cluster')
      else:
        pass
      if self.parent_nodes != [] and not self.stop_event.is_set() and not Node.rebuilding:
        heartbeat_send(self.parent_nodes[0], 'parent_nodes')
      else:
        pass

  # kills the node
  # throws a timeout exception on the caller side b/c the server is shut down
  def discard(self):
    def shutdown_node():
      print("shutting down node at", self.current_node['ip'], ":", self.current_node['port'])
      self.stop_event.set()
      self.srv.shutdown()
      self.heartbeat_thread.join(timeout=5)
      self.server_thread.join(timeout=5)
    
    # starting shutdown thread
    t = threading.Thread(target=shutdown_node, daemon=False)
    t.start()
    t.join(timeout=10)
    print("node at", self.current_node['ip'], ":", self.current_node['port'], "shut down successfully.")
    return True

  # Cluster size defined by cluster_size * replication_factor + 1. +1 for master.
  def __init__(self, node_type, sub_cluster, parent_nodes, ip, port, node_id=0, cluster_size=3, replication_factor=3, rebuild=False, datastore={}):
    self.current_node = {"node_type":"Chunk", "ip":None, "port":None, "node_id":None}
    self.sub_cluster = []
    self.parent_nodes = []
    self.node_id = node_id
    self.replication_factor = replication_factor
    self.cluster_size = cluster_size
    self.datastore = datastore
    
    self.sub_cluster += sub_cluster
    self.parent_nodes += parent_nodes
    self.sub_cluster = list(filter(None, self.sub_cluster))
    self.parent_nodes = list(filter(None, self.parent_nodes))
    self.current_node = {"node_type":node_type, "ip":ip, "port":port, "node_id":node_id}
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

    if(self.current_node['node_type'] == "Master" and self.sub_cluster == [] and not rebuild):
      print("Starting subcluster servers")
      Node.full_cluster = [None] * (1 + cluster_size * replication_factor)
      for i in range(cluster_size):
        n = Node("Chunk", [], [self.current_node], "localhost", port + i*replication_factor + 1, node_id=node_id + i*replication_factor + 1, cluster_size=cluster_size, replication_factor=replication_factor)
        self.sub_cluster.append(n.current_node)

    print("parent nodes:",self.parent_nodes, "sub cluster:", self.sub_cluster)
    if(self.current_node['node_type'] == "Chunk" and self.parent_nodes[0]["node_type"] == "Master" and not rebuild):
      print("Starting subcluster chunk servers")
      for i in range(replication_factor - 1):
        n = Node("Chunk", [], [] + [self.current_node] + self.parent_nodes, "localhost", port + i + 1, node_id= node_id + i + 1, cluster_size=cluster_size, replication_factor=replication_factor)
        #print(n.current_node, n.parent_nodes)
        self.sub_cluster.append(n.current_node)

    print(self.node_id, "has completed healing/rebuilding process." if rebuild else "has started")
    Node.full_cluster[self.node_id] = self.current_node

    self.stop_event = threading.Event()

    # begins the heartbeat thread
    self.heartbeat_thread = threading.Thread(target=self.heartbeat, daemon=True)
    self.heartbeat_thread.start()

    if rebuild:
      time.sleep(5)  # wait for heartbeat stabilization
      Node.rebuilding = False

    # if self.current_node['node_type'] == "Master":
    #   while True:
    #     time.sleep(1)


################
## Cold Start ##
################
rf = 3  # replication factor
cs = 3  # cluster size
master_thread = threading.Thread(target=lambda: Node(
  node_type="Master", 
  sub_cluster=[], 
  parent_nodes=[], 
  ip="localhost", 
  port=9000,
  cluster_size=cs,
  replication_factor=rf
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

print("Adding second data to cluster via master node...")
ret2 = master.add_data("second_key", "second_value")
print("Master returned:", ret2, "adding to node...")
if ret2["node_type"] == "Chunk":
  rq2 = client.ServerProxy(f"http://{ret2["ip"]}:{ret2["port"]}")
  rq2.add_data("second_key", "second_value")
  print("Second data added.")

# Delete the second key-value pair
print("Removing second data from cluster via master node...")
ret_delete = master.remove_data("second_key")
if ret2["node_type"] == "Chunk":
  rq2 = client.ServerProxy(f"http://{ret2['ip']}:{ret2['port']}")
  ret_delete_node = rq2.remove_data("second_key")
  print("Second key removed from chunk node.")

# Attempt to retrieve the deleted key
print("Requesting deleted data from cluster via master node...")
ret_deleted = master.get_data("second_key")
print("Master returned:", ret_deleted)
if ret_deleted["node_type"] == "Chunk":
  rq_deleted = client.ServerProxy(f"http://{ret_deleted['ip']}:{ret_deleted['port']}")
  deleted_data = rq_deleted.get_data("second_key")
  print("Deleted key retrieval result:", deleted_data)

print("Requesting data from cluster via master node...")
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
  try:
    rq.discard()
  except Exception as e:
    print("Node kill exception (expected):", e)

# keep main thread alive and do basic commands
print("Press 'q' to shut down the cluster.")
print("Press 'k' to kill another chunk node to test heartbeat recovery.")
print("Press 'space' to print the cluster structure.")
while True:
  time.sleep(0.1)
  if keyboard.is_pressed('q'):
    print("Shutting down cluster... This will take a few seconds.")
    master.discard()
    break
  if keyboard.is_pressed('k'):
    print("Killing a chunk node to test heartbeat recovery...")
    ret = master.get_data("example_key")
    rq = client.ServerProxy(f"http://{ret["ip"]}:{ret["port"]}")
    try:
      rq.discard()
    except Exception as e:
      print("Node kill exception (expected):", e)
  if keyboard.is_pressed('g'):
    print("Requesting data from cluster via master node...")
    ret = master.get_data("example_key")
    print("Retrieving data from chunk node", ret, "directly...")
    if ret["node_type"] == "Chunk":
      rq = client.ServerProxy(f"http://{ret["ip"]}:{ret["port"]}")
      data = rq.get_data("example_key")
      print("Data retrieved: ", data)
  if keyboard.is_pressed('space'):
    print("Printing cluster structure...")
    for node in Node.full_cluster:
      if node["node_id"] == 0:
        print("Master Node:", Node.full_cluster[0])
      elif node["node_id"] % rf == 1:
        print("\tChunk Node (Middle Layer):", node)
      else:
        print("\t\tChunk Node (Bottom Layer):", node)