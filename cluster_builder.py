from node import Node

master = Node("Master", None, None, "192.168.1.1", "9000")
chunk_master = Node("ChunkMaster", None, None, "192.168.1.2", "9002")
chunk = Node("Chunk", None, None, "192.168.1.3", "9004")

master.add_child(chunk_master.current_node)
chunk_master.add_child(chunk.current_node)

print("\n\n")

print(master.add_data("a", 2))
print(chunk_master.add_data("a", 2))
print(chunk.add_data("a", 2))

print(chunk.get_data("a"))
print(chunk.get_data("b"))
print(chunk.remove_data("a"))
print(chunk.get_data("a"))