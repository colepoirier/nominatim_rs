use crate::node::{Coordinate, NodeCoordDB, NodeTags};
use dashmap::DashMap;
use osm_pbf_iter::Way;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

// Array used to specify z_order per key/value combination.
// Each element has the form {key, value, z_order, is_road}.
// If is_road=1, the object will be added to planet_osm_roads.
lazy_static! {
    pub static ref ZORDERING_TAGS: HashSet<(&'static str, &'static str, i8, bool)> =
        HashSet::from_iter(vec![
            ("railway", "nil", 5, true),
            ("boundary", "administrative", 0, true),
            ("bridge", "yes", 10, false),
            ("bridge", "true", 10, false),
            ("bridge", "1", 10, false),
            ("tunnel", "yes", -10, false),
            ("tunnel", "true", -10, false),
            ("tunnel", "1", -10, false),
            ("highway", "minor", 3, false),
            ("highway", "road", 3, false),
            ("highway", "unclassified", 3, false),
            ("highway", "residential", 3, false),
            ("highway", "tertiary_link", 4, false),
            ("highway", "tertiary", 4, false),
            ("highway", "secondary_link", 6, true),
            ("highway", "secondary", 6, true),
            ("highway", "primary_link", 7, true),
            ("highway", "primary", 7, true),
            ("highway", "trunk_link", 8, true),
            ("highway", "trunk", 8, true),
            ("highway", "motorway_link", 9, true),
            ("highway", "motorway", 9, true),
        ]);
}

pub type RoadsDB = DashMap<u64, DebugWay>;

pub type WayDB = DashMap<u64, DebugWay>;

#[derive(Debug, PartialOrd, PartialEq, Clone)]
pub struct LineString {
    pub coords: Vec<Coordinate>,
}

#[derive(Debug, PartialOrd, PartialEq, Clone)]
pub enum WayProcessingError {
    LineStringCreationError(u64),
    ClosedLineStringCreationError(&'static str),
}

impl LineString {
    pub fn from_node_refs(
        way: &Way,
        node_coords_db: &NodeCoordDB,
    ) -> Result<LineString, WayProcessingError> {
        let mut line: Vec<Coordinate> = Vec::new();
        for node_id in way.refs().map(|i| i as u64) {
            if let Some(coord) = node_coords_db.get(&node_id) {
                line.push(Coordinate::from_db_coord(node_id, *coord.value()));
            } else {
                return Err(WayProcessingError::LineStringCreationError(node_id));
            }
        }
        Ok(LineString { coords: line })
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct ClosedLineString {
    coords: Vec<Coordinate>,
}

impl ClosedLineString {
    pub fn new(mut coords: Vec<Coordinate>) -> Result<Self, WayProcessingError> {
        if coords.len() < 3 {
            Err(WayProcessingError::ClosedLineStringCreationError("Cannot create a ClosedLineString from fewer than 3 coordinates. Try creating a Line instead."))
        } else {
            if coords.first().unwrap() == coords.last().unwrap() {
                Ok(ClosedLineString { coords: coords })
            } else {
                coords.push(*(coords.first().clone().unwrap()));
                Ok(ClosedLineString { coords: coords })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum CoordsShape {
    Linear(LineString),
    Polygonal(ClosedLineString),
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Area(ClosedLineString);

#[derive(Debug, Clone, PartialEq)]
pub struct DebugWay {
    id: u64,
    coords_shape: CoordsShape,
    tags: Option<HashMap<String, String>>,
}

impl PartialOrd for DebugWay {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

pub fn add_z_order(tags: &NodeTags) -> (Option<i8>, bool) {
    // The default z_order is 0
    let mut z_order: Option<i8> = None;
    let mut is_road: bool = false;

    if let Some(l) = tags.get("layer") {
        if let Ok(n) = l.parse::<i8>() {
            z_order = Some(10 * n);
        }
    }

    for (k, v, z, r) in ZORDERING_TAGS.iter() {
        if (v != &"nil" && tags.get(*k).is_some()) || (v == &"nil" && tags.get(*k).is_some()) {
            if *r {
                is_road = true;
            }
            match z_order {
                Some(n) => z_order = Some(n + *z),
                None => z_order = Some(*z),
            }
        }
    }

    (z_order, is_road)
}

pub fn process_way<'a>(
    way: &'a Way<'a>,
    generic_keys: &HashSet<&'static str>,
    node_coord_db: &NodeCoordDB,
    roads_db: &RoadsDB,
    way_db: &WayDB,
) -> Result<(), WayProcessingError> {
    let k = way.id;

    let mut tags: NodeTags = way
        .tags()
        .map(|(a, b)| (String::from(a), String::from(b)))
        .collect();

    let mut final_tags: Option<NodeTags> = None;
    let mut is_polygon = false;

    if tags.len() > 0 {
        let filtered_tag_keys: Vec<String> =
            HashSet::from_iter(way.tags().map(|(k, _v)| k).collect::<Vec<&str>>())
                .intersection(generic_keys)
                .cloned()
                .map(|s| String::from(s))
                .collect();

        if !filtered_tag_keys.is_empty() {
            for k in tags.keys().cloned().collect::<Vec<String>>() {
                if !filtered_tag_keys.contains(&k) {
                    tags.remove(&k);
                }
            }
        }
    }

    let (z_order, is_road) = add_z_order(&tags);

    if let Some(n) = z_order {
        tags.insert("z_order".to_string(), n.to_string());
    }

    if tags.len() > 0 {
        let filtered_polygon_keys: Vec<&str> =
            HashSet::from_iter(way.tags().map(|(k, _v)| k).collect::<Vec<&str>>())
                .intersection(generic_keys)
                .cloned()
                .collect();

        if filtered_polygon_keys.len() > 0 {
            is_polygon = true;
        }

        if let Some(v) = tags.get("area") {
            for x in ["yes", "1", "true"].iter() {
                if *x == v.as_str() {
                    is_polygon = true;
                }
            }

            for x in ["no", "0", "false"].iter() {
                if *x == v.as_str() {
                    is_polygon = false;
                }
            }
        }

        final_tags = Some(tags);
    }

    match LineString::from_node_refs(way, node_coord_db) {
        Ok(ls) => {
            let w: DebugWay;

            if is_polygon {
                match ClosedLineString::new(ls.coords) {
                    Ok(cls) => {
                        w = DebugWay {
                            id: way.id,
                            coords_shape: CoordsShape::Polygonal(cls),
                            tags: final_tags,
                        };
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            } else {
                w = DebugWay {
                    id: way.id,
                    coords_shape: CoordsShape::Linear(ls),
                    tags: final_tags,
                };
            }

            if is_road {
                roads_db.insert(k, w.clone());
            }

            way_db.insert(k, w);
            Ok(())
        }
        Err(e) => Err(e),
    }
}
