#![allow(unused_imports)]
extern crate geo;
extern crate osm_pbf_iter;
extern crate proj;

use osm_pbf_iter::*;
use proj::{Area, Proj};

use geo::{
    algorithm::area::Area, Geometry, GeometryCollection, Line, LineString, MultiLineString,
    MultiPoint, MultiPolygon, Point, Polygon, Rect, Triangle,
};

#[derive(Debug, Serialize, Deserialize, PartialOrd, PartialEq)]
pub struct Coordinate {
    lat: f64,
    lon: f64,
}

#[derive(Debug, PartialOrd, PartialEq)]
pub struct DebugWay {
    id: u64,
    refs: Vec<u64>,
}

fn a() -> DebugWay {
    DebugWay {
        id: 3498042,
        refs: vec![
            16798926, 2021991707, 2021992152, 2021992062, 2021992098, 16798927, 16798928, 16798929,
            2021991457, 16798930, 2021992309, 2021991672, 16798932, 2021991681, 2021991680,
            16798933, 2021992360, 2021992032, 16798934, 2021992406, 16798935, 2021991905, 16798936,
            20938616, 16798937, 16798938, 2021991620, 2021992217, 16798939, 2021991922, 16798940,
            2021991981, 16798941, 16798942, 16798943, 2021992454, 2021992413, 2021991444, 16798944,
            2021991939, 16798945, 919158340, 2054272886, 2054272883, 16798946, 16798947, 16798948,
            292219707, 16798949, 16798950, 2054203350, 2054203357, 2054203498, 2054203483,
            16798951, 16798952, 16798953, 2054203320, 2054203318, 2054272855, 16798954, 2054203410,
            16798955, 2054203526, 2054203344, 16798956, 2054203319, 2054203312, 2054203362,
            16798957, 2054203487, 2054203376, 2054203370, 16798958, 2054203403, 2054203537,
            16798959, 2054203436, 16798960, 16798962,
        ],
    }
}

pub fn linestring_from_way(id: u64, nodes: Vec<i64>) {
    let line_coords: Vec<Coordinate>;
    for node in nodes {
        line_coords.append(bincode::deserialize(db.get(node).unwrap()).unwrap().coord);
    }
}
