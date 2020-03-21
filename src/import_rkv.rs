#![allow(unused_imports)]
#![feature(option_expect_none)]
#[macro_use]
extern crate lazy_static;
extern crate geo;
extern crate proj;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate dashmap;
extern crate num_cpus;
extern crate osm_pbf_iter;
extern crate serde;

use dashmap::iter::{Iter, IterMut};
use dashmap::DashMap;

use std::cmp::{max, min};
use std::collections::{BTreeSet, HashMap};
use std::env::args;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::iter::FromIterator;
use std::prelude::*;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tempfile::Builder;

use osm_pbf_iter::*;

use proj::{Area, Proj};

use rkv::{Manager, Rkv, SingleStore, StoreOptions, Value};
use serde::{Deserialize, Serialize};

use geo::{
    Geometry, GeometryCollection, Line, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon, Rect, Triangle,
};

lazy_static! {
    pub static ref POLYGON_KEYS: BTreeSet<&'static str> = BTreeSet::from_iter(vec![
        "aeroway",
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
    pub static ref GENERIC_KEYS: BTreeSet<&'static str> = BTreeSet::from_iter(vec![
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
    pub static ref DELETE_TAGS: BTreeSet<&'static str> =
        BTreeSet::from_iter(vec!["FIXME", "note", "source",]);
}

// Array used to specify z_order per key/value combination.
// Each element has the form {key, value, z_order, is_road}.
// If is_road=1, the object will be added to planet_osm_roads.
lazy_static! {
    pub static ref ZORDERING_TAGS: BTreeSet<(&'static str, &'static str, i8, i8)> =
        BTreeSet::from_iter(vec![
            ("railway", "nil", 5, 1),
            ("boundary", "administrative", 0, 1),
            ("bridge", "yes", 10, 0),
            ("bridge", "true", 10, 0),
            ("bridge", "1", 10, 0),
            ("tunnel", "yes", -10, 0),
            ("tunnel", "true", -10, 0),
            ("tunnel", "1", -10, 0),
            ("highway", "minor", 3, 0),
            ("highway", "road", 3, 0),
            ("highway", "unclassified", 3, 0),
            ("highway", "residential", 3, 0),
            ("highway", "tertiary_link", 4, 0),
            ("highway", "tertiary", 4, 0),
            ("highway", "secondary_link", 6, 1),
            ("highway", "secondary", 6, 1),
            ("highway", "primary_link", 7, 1),
            ("highway", "primary", 7, 1),
            ("highway", "trunk_link", 8, 1),
            ("highway", "trunk", 8, 1),
            ("highway", "motorway_link", 9, 1),
            ("highway", "motorway", 9, 1),
        ]);
}

#[derive(Debug, Serialize, Deserialize, PartialOrd, PartialEq)]
pub struct Coordinate {
    lat: f64,
    lon: f64,
}

impl Coordinate {
    pub fn to_geo_coordinate(&self) -> geo::Coordinate<f64> {
        geo::Coordinate {
            x: self.lon,
            y: self.lat,
        }
    }
}

fn blobs_worker<'a>(
    req_rx: Receiver<Blob>,
    // db_conn: Sender<(String, Vec<u8>)>,
    node_coords: &DashMap<u64, Coordinate>,
    node_tags: &DashMap<u64, String>,
    // ret: Sender<HashMap<String, u64>>,
) {
    let mut early_termination = false;
    let mut tag_counts: HashMap<String, u64> = HashMap::new();
    loop {
        if early_termination {
            // let mut sorted_tag_counts = tag_counts
            //     .clone()
            //     .drain()
            //     .into_iter()
            //     .collect::<Vec<(String, u64)>>();
            // println!("Total tag count: {}", sorted_tag_counts.len());
            // sorted_tag_counts.sort_by(|l, r| r.1.partial_cmp(&l.1).unwrap());
            // sorted_tag_counts.truncate(100);
            // tag_counts = HashMap::from_iter(sorted_tag_counts);
            // println!("Disaggregated tag counts: {:?}.", &tag_counts);
            // ret.send(tag_counts);
            drop(req_rx);
            // drop(db_conn);
            // println!("Dropped wkr_rec and db_snd.");
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
                    node_coords.insert(
                        n.id,
                        Coordinate {
                            lat: n.lat,
                            lon: n.lon,
                        },
                    );
                    if n.tags.len() > 0 {
                        let mut tags: Vec<String> = vec![];
                        for tag in &n.tags {
                            tags.push(String::from(tag.0));
                        }
                        let tags: Vec<&str> =
                            tags.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
                        let filtered_tag_keys: Vec<&str> = BTreeSet::from_iter(tags)
                            .intersection(&GENERIC_KEYS)
                            .cloned()
                            .collect();
                        if filtered_tag_keys.len() < 1 {
                            continue;
                        }
                        // println!("{:?}", &filtered_tag_keys);
                        // for tag in &filtered_tag_keys {
                        //     // println!("{:?}.", tag);
                        //     *tag_counts.entry(String::from(*tag)).or_insert(1) += 1;
                        // }
                        let k = n.id;
                        let k = k.to_string();
                        let v = bincode::serialize(&filtered_tag_keys);
                        match v {
                            Ok(b) => {
                                // println!("{:?}", &b);
                                if let Err(e) = db_conn.send((k, b)) {
                                    eprintln!("{}", e);
                                    early_termination = true;
                                    break;
                                };
                            }
                            Err(e) => {
                                eprintln!(
                                    "Error {:?}, with serialization to bytes of: {:?}.",
                                    e, n
                                );
                                break;
                            }
                        }
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

// fn db_write_thread(req_rx: Receiver<(String, Vec<u8>)>) {
//     //
//     // First determine the path to the environment, which is represented
//     // on disk as a directory containing two files:
//     //
//     //   * a data file containing the key/value stores
//     //   * a lock file containing metadata about current transactions
//     //
//     // In this example, we use the `tempfile` crate to create the directory.
//     //
//     let root = Builder::new().prefix("simple-db").tempdir().unwrap();
//     fs::create_dir_all(root.path()).unwrap();
//     let path = root.path();

//     // The Manager enforces that each process opens the same environment
//     // at most once by caching a handle to each environment that it opens.
//     // Use it to retrieve the handle to an opened environment—or create one
//     // if it hasn't already been opened:
//     let mut env = Rkv::environment_builder();
//     env.set_max_dbs(100);
//     env.set_max_readers(100);
//     env.set_map_size(10_240_000_000);
//     let mut flags = rkv::EnvironmentFlags::empty();
//     flags.set(rkv::EnvironmentFlags::WRITE_MAP, true);
//     flags.set(rkv::EnvironmentFlags::MAP_ASYNC, true);
//     env.set_flags(flags);
//     let created_arc = Manager::singleton()
//         .write()
//         .unwrap()
//         .get_or_create(path, |p| Rkv::from_env(p, env))
//         .unwrap();
//     let env = created_arc.read().unwrap();

//     // Then you can use the environment handle to get a handle to a datastore:
//     let store: SingleStore = env.open_single("nodes", StoreOptions::create()).unwrap();
//     thread::sleep(Duration::from_secs(15));
//     let mut node_tags: HashMap<u64, Vec<u8>> = HashMap::new();
//     let t1 = std::time::Instant::now();
//     {
//         // Use a write transaction to mutate the store via a `Writer`.
//         // There can be only one writer for a given environment, so opening
//         // a second one will block until the first completes.
//         let mut writer = env.write().unwrap();

//         let mut num_objects: u64 = 0;
//         let mut total_size: u64 = 0;
//         let mut max_size: u64 = 0;
//         let mut min_size: u64 = 10_000;

//         loop {
//             match req_rx.try_recv() {
//                 Ok((k, v)) => {
//                     let size = v.capacity() as u64;
//                     total_size += size;
//                     max_size = max(max_size, size);
//                     min_size = min(min_size, size);
//                     node_tags
//                         .insert(k.parse::<u64>().unwrap(), v)
//                         .expect_none(&format!("Duplicate key. Object number: {}.", num_objects));
//                     num_objects += 1;
//                     // match store.put(&mut writer, k, &Value::Blob(&v[..])) {
//                     //     Ok(_) => num_objects += 1,
//                     //     Err(e) => eprintln!("Error: {:?}.", e),
//                     // }

//                     // if num_objects % 5_000_000 == 0 {
//                     //     writer.commit().expect("Commit store error.");
//                     //     writer = env.write().unwrap();
//                     //     println!("Done {} objects.", num_objects);
//                     // };
//                 }
//                 Err(TryRecvError::Disconnected) => {
//                     eprintln!("DB all senders disconnected");
//                     writer.commit().expect("Commit store error.");
//                     println!("Done {} objects.", num_objects);
//                     let stats = env.stat().unwrap();
//                     println!(
//                         "\n
// Page size in bytes: {}.
// B-tree depth: {}.
// Number of internal (non-leaf) pages: {}.
// Number of leaf pages: {}.
// Number of overflow pages: {}.
// Number of data entries: {}.
// Load ratio: {}.
//                         \n",
//                         stats.page_size(),
//                         stats.depth(),
//                         stats.branch_pages(),
//                         stats.leaf_pages(),
//                         stats.overflow_pages(),
//                         stats.entries(),
//                         env.load_ratio().unwrap(),
//                     );
//                     break;
//                 }
//                 Err(TryRecvError::Empty) => continue,
//             };
//         }
//         let elapsed = t1.elapsed();
//         println!("DB write time: {} s. DB total_objects_written: {}. DB total_size_written: {} MB. DB avg_object_size: {}. DB min_object_size: {}. DB max_object_size: {}. DB avg_time_per object: {} us. DB avg_write_speed: {} objects/s, {} MB/s.",
//         elapsed.as_secs_f64(), num_objects, total_size / 1_000_000, total_size / num_objects, min_size, max_size, elapsed.as_micros() as f64 / num_objects as f64, num_objects as f64 / elapsed.as_secs_f64(), total_size as f64 / 1_000_000.0 / elapsed.as_secs_f64() );
//     }
// }

fn process() {
    let cpus = num_cpus::get();

    for arg in args().skip(1) {
        let node_coords: Arc<DashMap<u64, Coordinate>> =
            Arc::from(DashMap::with_capacity(140_000_000));
        let mut workers = Vec::with_capacity(cpus);
        let (db_snd, db_rec) = channel::<(String, Vec<u8>)>();
        let db_thread = std::thread::spawn(move || {
            db_write_thread(db_rec);
        });
        let (stats_snd, stats_rec) = channel::<HashMap<String, u64>>();
        for _ in 0..cpus {
            let db_snd = db_snd.clone();
            let stats_snd = stats_snd.clone();
            let node_coords = node_coords.clone();
            let (wkr_snd, wkr_rec) = channel();
            workers.push(wkr_snd);
            thread::spawn(move || {
                blobs_worker(wkr_rec, db_snd, &node_coords, stats_snd);
            });
        }
        drop(db_snd);
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
        let mut tag_counts: HashMap<String, u64> = HashMap::new();
        for (n, worker) in workers.iter().enumerate() {
            println!("Dropping worker: {}. {:?}.", n, worker);
            let mut stats = stats_rec.recv().unwrap();
            for (tag, cnt) in stats.drain().into_iter() {
                *tag_counts.entry(tag).or_insert(cnt) += cnt;
            }
            drop(worker);
        }
        let mut tag_counts = tag_counts
            .drain()
            .into_iter()
            .collect::<Vec<(String, u64)>>();
        println!("Total tag count: {}", tag_counts.len());
        tag_counts.sort_by(|l, r| r.1.partial_cmp(&l.1).unwrap());
        tag_counts.truncate(100);
        println!("Disaggregated tag counts: {:?}.", tag_counts);
        println!("Dropped all workers.");
        println!("node_coords size: {}.", node_coords.len());
        db_thread.join().unwrap();
        let stop = Instant::now();
        let duration = stop.duration_since(start);
        let duration = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
        let mut f = reader.into_inner();
        match f.seek(SeekFrom::Current(0)) {
            Ok(pos) => {
                let rate = pos as f64 / 1024f64 / 1024f64 / duration;
                println!(
                    "Processed {} MB in {:.2} seconds ({:.2} MB/s)",
                    pos / 1024 / 1024,
                    duration,
                    rate
                );
            }
            Err(_) => (),
        }
    }
}

fn main() {
    let t1 = std::time::Instant::now();
    process();

    let write_time = t1.elapsed();
    println!(
        "Write: {:?}. Read: {:?}. Total: {:?}.",
        write_time,
        t1.elapsed() - write_time,
        t1.elapsed()
    );
}

// pub struct DebugLineString {
//     pub id: u64,
//     pub linestring: LineString<f64>,
// }

// impl DebugLineString {
//     pub fn linestring_from_way(
//         db: SingleStore,
//         reader: &rkv::Reader,
//         id: u64,
//         nodes: Vec<i64>,
//     ) -> Self {
//         let mut line_coords: Vec<geo::Coordinate<f64>> = vec![];
//         for node in nodes {
//             if let Some(Value::Blob(v)) = db.get(reader, node.to_string()).unwrap() {
//                 line_coords.push(
//                     bincode::deserialize::<DebugNode>(v)
//                         .unwrap()
//                         .coord
//                         .to_geo_coordinate(),
//                 );
//             }
//         }
//         DebugLineString {
//             id: id,
//             linestring: LineString::from(line_coords),
//         }
//     }
// }

//     // Keys are `AsRef<u8>`, and the return value is `Result<Option<Value>, StoreError>`.
//     println!("Get int {:?}", store.get(&reader, "int").unwrap());
//     println!("Get uint {:?}", store.get(&reader, "uint").unwrap());
//     println!("Get float {:?}", store.get(&reader, "float").unwrap());
//     println!("Get instant {:?}", store.get(&reader, "instant").unwrap());
//     println!("Get boolean {:?}", store.get(&reader, "boolean").unwrap());
//     println!("Get string {:?}", store.get(&reader, "string").unwrap());
//     println!("Get json {:?}", store.get(&reader, "json").unwrap());
//     println!("Get blob {:?}", store.get(&reader, "blob").unwrap());

//     // Retrieving a non-existent value returns `Ok(None)`.
//     println!(
//         "Get non-existent value {:?}",
//         store.get(&reader, "non-existent").unwrap()
//     );

//     // A read transaction will automatically close once the reader
//     // goes out of scope, so isn't necessary to close it explicitly,
//     // although you can do so by calling `Reader.abort()`.
// }

// {
//     // Aborting a write transaction rolls back the change(s).
//     let mut writer = env.write().unwrap();
//     store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
//     writer.abort();
//     let reader = env.read().expect("reader");
//     println!(
//         "It should be None! ({:?})",
//         store.get(&reader, "foo").unwrap()
//     );
// }

// {
//     // Explicitly aborting a transaction is not required unless an early
//     // abort is desired, since both read and write transactions will
//     // implicitly be aborted once they go out of scope.
//     {
//         let mut writer = env.write().unwrap();
//         store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
//     }
//     let reader = env.read().expect("reader");
//     println!(
//         "It should be None! ({:?})",
//         store.get(&reader, "foo").unwrap()
//     );
// }

// {
//     // Deleting a key/value pair also requires a write transaction.
//     let mut writer = env.write().unwrap();
//     store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
//     store.put(&mut writer, "bar", &Value::Str("baz")).unwrap();
//     store.delete(&mut writer, "foo").unwrap();

//     // A write transaction also supports reading, and the version of the
//     // store that it reads includes the changes it has made regardless of
//     // the commit state of that transaction.
//     // In the code above, "foo" and "bar" were put into the store,
//     // then "foo" was deleted so only "bar" will return a result when the
//     // database is queried via the writer.
//     println!(
//         "It should be None! ({:?})",
//         store.get(&writer, "foo").unwrap()
//     );
//     println!("Get bar ({:?})", store.get(&writer, "bar").unwrap());

//     // But a reader won't see that change until the write transaction
//     // is committed.
//     {
//         let reader = env.read().expect("reader");
//         println!("Get foo {:?}", store.get(&reader, "foo").unwrap());
//         println!("Get bar {:?}", store.get(&reader, "bar").unwrap());
//     }
//     writer.commit().unwrap();
//     {
//         let reader = env.read().expect("reader");
//         println!(
//             "It should be None! ({:?})",
//             store.get(&reader, "foo").unwrap()
//         );
//         println!("Get bar {:?}", store.get(&reader, "bar").unwrap());
//     }

//     // Committing a transaction consumes the writer, preventing you
//     // from reusing it by failing at compile time with an error.
//     // This line would report error[E0382]: borrow of moved value: `writer`.
//     // store.put(&mut writer, "baz", &Value::Str("buz")).unwrap();
// }

// {
//     // Clearing all the entries in the store with a write transaction.
//     {
//         let mut writer = env.write().unwrap();
//         store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
//         store.put(&mut writer, "bar", &Value::Str("baz")).unwrap();
//         writer.commit().unwrap();
//     }

//     {
//         let mut writer = env.write().unwrap();
//         store.clear(&mut writer).unwrap();
//         writer.commit().unwrap();
//     }

//     {
//         let reader = env.read().expect("reader");
//         println!(
//             "It should be None! ({:?})",
//             store.get(&reader, "foo").unwrap()
//         );
//         println!(
//             "It should be None! ({:?})",
//             store.get(&reader, "bar").unwrap()
//         );
//     }
// }
