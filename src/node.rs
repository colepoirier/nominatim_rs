use dashmap::DashMap;
use osm_pbf_iter::*;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

pub type NodeCoordDB = DashMap<u64, __DBCoordinate>;
pub type NodeTags = HashMap<String, String>;

pub type NodeTagsDB = DashMap<u64, HashMap<String, String>>;

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub struct __DBCoordinate {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub struct Coordinate {
    pub id: u64,
    pub lat: f64,
    pub lon: f64,
}

impl Coordinate {
    pub fn from_db_coord(id: u64, db_coord: __DBCoordinate) -> Self {
        Coordinate {
            id: id,
            lat: db_coord.lat,
            lon: db_coord.lon,
        }
    }
}

pub fn process_node<'a>(
    n: &'a Node<'a>,
    node_coord_db: &NodeCoordDB,
    generic_keys: &HashSet<&'static str>,
    node_tags_db: &NodeTagsDB,
) -> Option<u64> {
    node_coord_db.insert(
        n.id,
        __DBCoordinate {
            lat: n.lat,
            lon: n.lon,
        },
    );
    if n.tags.len() > 0 {
        let tags: HashMap<String, String> = HashMap::from_iter(
            n.tags
                .iter()
                .map(|(k, v)| (String::from(*k), String::from(*v))),
        );

        let keys: Vec<&str> = tags.iter().map(|(k, _v)| k.as_str()).collect::<Vec<&str>>();

        let filtered_tag_keys: Vec<String> = HashSet::from_iter(keys)
            .intersection(generic_keys)
            .cloned()
            .map(|s| String::from(s))
            .collect();

        if filtered_tag_keys.len() < 1 {
            return None;
        }

        let mut filtered_tags: HashMap<String, String> = HashMap::new();
        for k in filtered_tag_keys {
            if let Some(v) = tags.get(&k) {
                filtered_tags.insert(k, v.to_string());
            }
        }

        let mut size: u64 = 0;
        for (_k, v) in filtered_tags.iter() {
            size += v.as_bytes().len() as u64;
        }

        node_tags_db.insert(n.id, filtered_tags);
        Some(size)
    } else {
        return None;
    }
}
