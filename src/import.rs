#![allow(unused_imports)]
#[macro_use]
extern crate lazy_static;
extern crate atomic_counter;
extern crate dashmap;
extern crate num_cpus;
extern crate osm_pbf_iter;

use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::env::args;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::iter::FromIterator;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use atomic_counter::{AtomicCounter, RelaxedCounter};
use dashmap::DashMap;
use osm_pbf_iter::*;

mod node;
use node::{process_node, NodeCoordDB, NodeTags, NodeTagsDB};

mod way;
use way::{process_way, DebugWay, LineString, RoadsDB, WayDB, WayProcessingError};

lazy_static! {
    pub static ref NODE_COORD_DB: Arc<NodeCoordDB> = Arc::from(DashMap::with_capacity(5_000_000));
}

lazy_static! {
    pub static ref ROADS_DB: Arc<RoadsDB> = Arc::from(DashMap::with_capacity(5_000_000));
}

lazy_static! {
    pub static ref NODE_TAGS_DB: Arc<NodeTagsDB> = Arc::from(DashMap::with_capacity(5_000_000));
}

lazy_static! {
    pub static ref WAY_DB: Arc<WayDB> = Arc::from(DashMap::with_capacity(50_000_000));
}

lazy_static! {
    pub static ref POLYGON_KEYS: HashSet<&'static str> = HashSet::from_iter(vec![
        "aeroway",
        "abandoned:aeroway",
        "abandoned:amenity",
        "abandoned:building",
        "abandoned:landuse",
        "abandoned:power",
        "area:highway",
        "amenity",
        "building",
        "harbour",
        "historic",
        "landuse",
        "leisure",
        "man_made",
        "military",
        "natural",
        "office",
        "place",
        "power",
        "public_transport",
        "shop",
        "sport",
        "tourism",
        "water",
        "waterway",
        "wetland",
    ]);
}

lazy_static! {
    pub static ref GENERIC_KEYS: HashSet<&'static str> = HashSet::from_iter(vec![
        "access",
        "addr:housename",
        "addr:housenumber",
        "addr:interpolation",
        "admin_level",
        "aerialway",
        "aeroway",
        "amenity",
        "area",
        "barrier",
        "bicycle",
        "boundary",
        "brand",
        "bridge",
        "building",
        "capital",
        "construction",
        "covered",
        "culvert",
        "cutting",
        "denomination",
        "disused",
        "ele",
        "embarkment",
        "foot",
        "generation:source",
        "harbour",
        "highway",
        "historic",
        "hours",
        "intermittent",
        "junction",
        "landuse",
        "layer",
        "leisure",
        "lock",
        "man_made",
        "military",
        "motor_car",
        "name",
        "natural",
        "office",
        "oneway",
        "operator",
        "place",
        "population",
        "power",
        "power_source",
        "public_transport",
        "railway",
        "ref",
        "religion",
        "route",
        "service",
        "shop",
        "sport",
        "surface",
        "toll",
        "tourism",
        "tower:type",
        "tracktype",
        "tunnel",
        "type",
        "water",
        "waterway",
        "wetland",
        "width",
        "wood",
    ]);
}

lazy_static! {
    pub static ref DELETE_TAGS: HashSet<&'static str> =
        HashSet::from_iter(vec!["FIXME", "note", "source",]);
}

lazy_static! {
    pub static ref WAYS_GOOD_COUNT: RelaxedCounter = RelaxedCounter::new(0);
}

lazy_static! {
    pub static ref WAYS_ERROR_COUNT: RelaxedCounter = RelaxedCounter::new(0);
}

fn collapse_this_comment() {
    // OsmType  Tag          DataType     Flags
    // node,way   access       text         linear
    // node,way   addr:housename      text  linear
    // node,way   addr:housenumber    text  linear
    // node,way   addr:interpolation  text  linear
    // node,way   admin_level  text         linear
    // node,way   aerialway    text         linear
    // node,way   aeroway      text         polygon
    // node,way   amenity      text         polygon
    // node,way   area         text         polygon # hard coded support for area=1/yes => polygon is in osm2pgsql
    // node,way   barrier      text         linear
    // node,way   bicycle      text         linear
    // node,way   brand        text         linear
    // node,way   bridge       text         linear
    // node,way   boundary     text         linear
    // node,way   building     text         polygon
    // node       capital      text         linear
    // node,way   construction text         linear
    // node,way   covered      text         linear
    // node,way   culvert      text         linear
    // node,way   cutting      text         linear
    // node,way   denomination text         linear
    // node,way   disused      text         linear
    // node       ele          text         linear
    // node,way   embankment   text         linear
    // node,way   foot         text         linear
    // node,way   generator:source    text  linear
    // node,way   harbour      text         polygon
    // node,way   highway      text         linear
    // node,way   historic     text         polygon
    // node,way   horse        text         linear
    // node,way   intermittent text         linear
    // node,way   junction     text         linear
    // node,way   landuse      text         polygon
    // node,way   layer        text         linear
    // node,way   leisure      text         polygon
    // node,way   lock         text         linear
    // node,way   man_made     text         polygon
    // node,way   military     text         polygon
    // node,way   motorcar     text         linear
    // node,way   name         text         linear
    // node,way   natural      text         polygon  # natural=coastline tags are discarded by a hard coded rule in osm2pgsql
    // node,way   office       text         polygon
    // node,way   oneway       text         linear
    // node,way   operator     text         linear
    // node,way   place        text         polygon
    // node,way   population   text         linear
    // node,way   power        text         polygon
    // node,way   power_source text         linear
    // node,way   public_transport text     polygon
    // node,way   railway      text         linear
    // node,way   ref          text         linear
    // node,way   religion     text         linear
    // node,way   route        text         linear
    // node,way   service      text         linear
    // node,way   shop         text         polygon
    // node,way   sport        text         polygon
    // node,way   surface      text         linear
    // node,way   toll         text         linear
    // node,way   tourism      text         polygon
    // node,way   tower:type   text         linear
    // way        tracktype    text         linear
    // node,way   tunnel       text         linear
    // node,way   water        text         polygon
    // node,way   waterway     text         polygon
    // node,way   wetland      text         polygon
    // node,way   width        text         linear
    // node,way   wood         text         linear
    // node,way   z_order      int4         linear # This is calculated during import
    // way        way_area     real         linear # This is calculated during import

    // pub fn is_area<'a>(way: &Way<'a>) -> bool {

    // }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub struct DebugStats {
    pub num_tags_objects: u64,
    pub total_size: u64,
    pub min_size: u64,
    pub max_size: u64,
}

pub fn blobs_worker<'a>(req_rx: Receiver<Blob>, stats: Sender<DebugStats>) {
    let mut early_termination = false;
    let mut debug_stats = DebugStats {
        num_tags_objects: 0,
        total_size: 0,
        min_size: 100,
        max_size: 0,
    };
    loop {
        if early_termination {
            drop(req_rx);
            stats
                .send(debug_stats)
                .expect("stats Reciever disconnected.");
            println!("Worker exit.");
            break;
        }

        let blob = match req_rx.try_recv() {
            Ok(blob) => blob,
            Err(TryRecvError::Empty) => continue,
            Err(TryRecvError::Disconnected) => {
                println!("Worker sender disconneced.");
                break;
            }
        };

        let data = blob.into_data();
        let primitive_block = PrimitiveBlock::parse(&data);
        for primitive in primitive_block.primitives() {
            match primitive {
                Primitive::Node(n) => {
                    if let Some(n) = process_node(&n, &NODE_COORD_DB, &GENERIC_KEYS, &NODE_TAGS_DB)
                    {
                        debug_stats.total_size += n;
                        debug_stats.num_tags_objects += 1;
                        debug_stats.min_size = min(debug_stats.min_size, n);
                        debug_stats.max_size = max(debug_stats.max_size, n);
                    }
                }
                Primitive::Way(w) => {
                    if let Err(e) =
                        process_way(&w, &GENERIC_KEYS, &NODE_COORD_DB, &ROADS_DB, &WAY_DB)
                    {
                        eprintln!("Could not process {:?}, because {:?} could not be found in the NODE_COORDS_DB.", w, e);
                        WAYS_ERROR_COUNT.inc();
                    } else {
                        WAYS_GOOD_COUNT.inc();
                    }
                }
                _ => {
                    early_termination = true;
                    break;
                }
            }
        }
    }
}

fn process() {
    let cpus = num_cpus::get();

    for arg in args().skip(1) {
        let mut workers = Vec::with_capacity(cpus);
        let (stats_snd, stats_rec) = channel();

        for _ in 0..cpus {
            let (wkr_snd, wkr_rec) = channel();
            let stats_snd = stats_snd.clone();

            workers.push(wkr_snd);

            thread::spawn(move || {
                blobs_worker(wkr_rec, stats_snd);
            });
        }

        println!("Open {}", arg);
        let f = File::open(&arg).unwrap();
        let mut reader = BlobReader::new(BufReader::new(f));
        let start = Instant::now();

        let mut w = 0;
        for blob in &mut reader {
            let req_tx = &workers[w];
            w = (w + 1) % cpus;

            if let Err(e) = req_tx.send(blob) {
                eprintln!("Error sending blob to worker: {:?}.", e);
                break;
            };
        }

        let mut debug_stats = DebugStats {
            num_tags_objects: 0,
            total_size: 0,
            min_size: 100,
            max_size: 0,
        };

        for (n, worker) in workers.iter().enumerate() {
            println!("Dropping worker: {}. {:?}.", n, worker);
            let stats = stats_rec.recv().unwrap();
            debug_stats.num_tags_objects += stats.num_tags_objects;
            debug_stats.total_size += stats.total_size;
            debug_stats.min_size = min(debug_stats.min_size, stats.min_size);
            debug_stats.max_size = max(debug_stats.max_size, stats.max_size);
            drop(worker);
        }

        let total_size_mb = debug_stats.total_size as f64 / 1_000_000.0;

        let stop = Instant::now();
        let duration = stop.duration_since(start);
        let secs = duration.as_secs_f64();
        let mut f = reader.into_inner();
        match f.seek(SeekFrom::Current(0)) {
            Ok(pos) => {
                let rate = pos as f64 / 1024f64 / 1024f64 / secs;
                println!(
                    "Processed {} MB raw osm.pbf data in {:.2} seconds ({:.2} MB/s).",
                    pos / 1024 / 1024,
                    secs,
                    rate
                );
            }
            Err(_) => (),
        }
        println!("node_coords entry count: {}.", NODE_COORD_DB.len());
        println!("Num node tags objects: {}.", debug_stats.num_tags_objects);
        println!("Total size of all tags: {} MB.", total_size_mb);
        println!("Good ways count: {}.", WAYS_GOOD_COUNT.get());
        println!("Error ways count: {}.", WAYS_ERROR_COUNT.get());
        println!("Ways db len: {}.", WAY_DB.len());
        println!("Roads db len: {}.", ROADS_DB.len());
        println!("min_size: {}.", debug_stats.min_size);
        println!("max_size: {}.", debug_stats.max_size);
        println!(
            "Avg obj processing rate: {:.3} objs/s.",
            debug_stats.num_tags_objects as f64 / secs
        );
        println!(
            "Avg obj processing rate: {:.6} MB/s.",
            total_size_mb as f64 / secs
        );
        println!(
            "Avg processing time per obj: {:.3} us.",
            duration.as_micros() as f64 / debug_stats.num_tags_objects as f64
        );
        println!(
            "Avg processing time per MB of objs: {:.6} ms.",
            duration.as_millis() as f64 / total_size_mb
        );
    }
}

fn main() {
    process();
}
