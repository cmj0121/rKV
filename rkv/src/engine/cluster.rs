use std::collections::HashMap;

/// Maps namespaces to shard groups for request routing.
#[derive(Clone, Debug)]
pub struct RoutingTable {
    pub version: u64,
    pub routes: HashMap<String, ShardGroup>,
    pub default_group: ShardGroup,
}

impl RoutingTable {
    /// Create a new routing table with the given default shard group.
    pub fn new(default_group: ShardGroup) -> Self {
        Self {
            version: 0,
            routes: HashMap::new(),
            default_group,
        }
    }

    /// Look up the shard group for a namespace.
    /// Returns the explicitly mapped group, or the default group if unmapped.
    pub fn lookup(&self, namespace: &str) -> &ShardGroup {
        self.routes.get(namespace).unwrap_or(&self.default_group)
    }

    /// Update the namespace-to-shard mapping and bump the version.
    pub fn set_route(&mut self, namespace: String, group: ShardGroup) {
        self.routes.insert(namespace, group);
        self.version += 1;
    }

    /// Remove a namespace mapping (falls back to default group).
    pub fn remove_route(&mut self, namespace: &str) {
        self.routes.remove(namespace);
        self.version += 1;
    }
}

/// A group of nodes that collectively own a set of namespaces.
#[derive(Clone, Debug)]
pub struct ShardGroup {
    pub id: u16,
    pub nodes: Vec<NodeInfo>,
}

impl ShardGroup {
    /// Create a new shard group with no nodes.
    pub fn new(id: u16) -> Self {
        Self {
            id,
            nodes: Vec::new(),
        }
    }

    /// Return the first healthy node, if any.
    pub fn healthy_node(&self) -> Option<&NodeInfo> {
        self.nodes.iter().find(|n| n.healthy)
    }
}

/// A node in the cluster with health tracking.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub addr: String,
    pub cluster_id: u16,
    pub healthy: bool,
}

impl NodeInfo {
    pub fn new(addr: impl Into<String>, cluster_id: u16) -> Self {
        Self {
            addr: addr.into(),
            cluster_id,
            healthy: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_table_lookup_default() {
        let rt = RoutingTable::new(ShardGroup::new(1));
        assert_eq!(rt.lookup("unknown").id, 1);
    }

    #[test]
    fn routing_table_lookup_explicit() {
        let mut rt = RoutingTable::new(ShardGroup::new(1));
        rt.set_route("users".to_string(), ShardGroup::new(2));
        assert_eq!(rt.lookup("users").id, 2);
        assert_eq!(rt.lookup("other").id, 1);
        assert_eq!(rt.version, 1);
    }

    #[test]
    fn routing_table_remove() {
        let mut rt = RoutingTable::new(ShardGroup::new(1));
        rt.set_route("users".to_string(), ShardGroup::new(2));
        rt.remove_route("users");
        assert_eq!(rt.lookup("users").id, 1);
        assert_eq!(rt.version, 2);
    }

    #[test]
    fn shard_group_healthy_node() {
        let mut sg = ShardGroup::new(1);
        sg.nodes.push(NodeInfo {
            addr: "10.0.0.1:8321".to_string(),
            cluster_id: 1,
            healthy: false,
        });
        sg.nodes.push(NodeInfo {
            addr: "10.0.0.2:8321".to_string(),
            cluster_id: 2,
            healthy: true,
        });
        let node = sg.healthy_node().unwrap();
        assert_eq!(node.addr, "10.0.0.2:8321");
    }

    #[test]
    fn shard_group_no_healthy_node() {
        let mut sg = ShardGroup::new(1);
        sg.nodes.push(NodeInfo {
            addr: "10.0.0.1:8321".to_string(),
            cluster_id: 1,
            healthy: false,
        });
        assert!(sg.healthy_node().is_none());
    }
}
