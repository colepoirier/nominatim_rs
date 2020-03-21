extern crate num_cpus;
extern crate osm_pbf_iter;
#[macro_use]
extern crate lazy_static;

use std::collections::BTreeSet;
use std::env::args;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::iter::FromIterator;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::Instant;

use osm_pbf_iter::*;

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

// fn blobs_worker(req_rx: Receiver<Blob>, res_tx: SyncSender<HashSet<String>>) {
//     let mut tags_set: HashSet<String> = HashSet::new();

//     loop {
//         let blob = match req_rx.recv() {
//             Ok(blob) => blob,
//             Err(_) => break,
//         };

//         let data = blob.into_data();
//         let primitive_block = PrimitiveBlock::parse(&data);
//         for primitive in primitive_block.primitives() {
//             match primitive {
//                 Primitive::Node(x) => {
//                     for tobj in x.tags.iter() {
//                         tags_set.insert(String::from((*tobj).0));
//                     }
//                 }
//                 Primitive::Way(x) => {
//                     for tobj in x.tags() {
//                         tags_set.insert(String::from((tobj).0));
//                     }
//                 }
//                 Primitive::Relation(x) => {
//                     for tobj in x.tags() {
//                         tags_set.insert(String::from((tobj).0));
//                     }
//                 }
//             }
//         }
//     }

//     res_tx.send(tags_set).unwrap();
// }

// pub fn add_z_order(tags: Vec<&str>)
//     -- The default z_order is 0
//     z_order = 0

//     -- Add the value of the layer key times 10 to z_order
//     if (keyvalues["layer"] ~= nil and tonumber(keyvalues["layer"])) then
//        z_order = 10*keyvalues["layer"]
//     }

//    -- Increase or decrease z_order based on the specific key/value combination as specified in zordering_tags
//     for i,k in ipairs(zordering_tags) do
//         -- If the value in zordering_tags is specified, match key and value. Otherwise, match key only.
//         if ((k[2]  and keyvalues[k[1]] == k[2]) or (k[2] == nil and keyvalues[k[1]] ~= nil)) then
//             -- If the fourth component of the element of zordering_tags is 1, add the object to planet_osm_roads
//             if (k[4] == 1) then
//                 roads = 1
//             }
//             z_order = z_order + k[3]
//         }
//     }

//     -- Add z_order as key/value combination
//     keyvalues["z_order"] = z_order

//     return keyvalues, roads
// }

fn blobs_worker(req_rx: Receiver<Blob>, res_tx: Sender<(f64, [u64; 3], [u64; 3], u64, u64)>) {
    let mut diff: f64 = 0.0;
    // counts: [nodes, ways, rels];
    let mut empty_count: [u64; 3] = [0; 3];
    let mut obj_count: [u64; 3] = [0; 3];
    let mut processed_node_tagged_count: u64 = 0;
    let mut processed_node_empty_count: u64 = 0;

    loop {
        let blob = match req_rx.recv() {
            Ok(blob) => blob,
            Err(_) => break,
        };

        let data = blob.into_data();
        let primitive_block = PrimitiveBlock::parse(&data);
        for primitive in primitive_block.primitives() {
            match primitive {
                Primitive::Node(x) => {
                    if x.tags.len() < 1 {
                        empty_count[0] += 1;
                        if obj_count[0] % 1_000_000 == 0 {
                            println!("{:?}", x);
                        }
                        continue;
                    }
                    let mut tags: Vec<&str> = vec![];
                    x.tags.clone().iter().for_each(|(a, b)| {
                        tags.push(a);
                        tags.push(b);
                    });
                    for tag in tags.iter() {
                        if (&POLYGON_KEYS).contains(tag) || (&GENERIC_KEYS).contains(tag) {
                            processed_node_tagged_count += 1;
                        }
                    }
                    // let set = BTreeSet::from_iter(tags);
                    // let olen: f64 = set.len() as f64;
                    // let mut poly_keys_intersection: Vec<&str> = (&set)
                    //     .intersection(&POLYGON_KEYS)
                    //     .cloned()
                    //     .collect::<Vec<&str>>();
                    // let mut generic_keys_intersection: Vec<&str> = set
                    //     .intersection(&POLYGON_KEYS)
                    //     .cloned()
                    //     .collect::<Vec<&str>>();
                    // (&mut poly_keys_intersection).append(&mut generic_keys_intersection);
                    // let intersection = BTreeSet::from_iter(poly_keys_intersection);
                    // let nlen: f64 = intersection.len() as f64;
                    // if olen == 0_f64 || nlen == 0_f64 {
                    //     empty_count[0] += 1;
                    //     continue;
                    // }
                    if tags.len() < 1 {
                        processed_node_empty_count += 1;
                        continue;
                    }
                    // diff += nlen / olen;
                    if obj_count[0] % 1_000_000 == 0 {
                        println!("Node: {:?}.", x,);
                    }
                    obj_count[0] += 1;
                }
                Primitive::Way(x) => {
                    let set = BTreeSet::from_iter(x.tags().map(|x| x.0));
                    let olen: f64 = set.len() as f64;
                    let intersection: Vec<&str> = (&set)
                        .intersection(&GENERIC_KEYS)
                        .cloned()
                        .collect::<Vec<&str>>();
                    let nlen: f64 = intersection.len() as f64;
                    if olen == 0_f64 || nlen == 0_f64 {
                        empty_count[1] += 1;
                        continue;
                    }
                    diff += nlen / olen;
                    if obj_count[1] % 1_400_000 == 0 {
                        println!(
                            "Way. Original tags: {:?}. Intersection tags: {:?}.",
                            set, intersection
                        );
                        println!("{:?}", x);
                    }
                    obj_count[1] += 1;
                }
                Primitive::Relation(x) => {
                    let set = BTreeSet::from_iter(x.tags().map(|x| x.0));
                    let olen: f64 = set.len() as f64;
                    let intersection: Vec<&&str> =
                        set.intersection(&GENERIC_KEYS).collect::<Vec<&&str>>();
                    let nlen: f64 = intersection.len() as f64;
                    if olen == 0_f64 || nlen == 0_f64 {
                        empty_count[2] += 1;
                        continue;
                    }
                    diff += nlen / olen;
                    if obj_count[2] % 20_000 == 0 {
                        println!(
                            "Rel. Original tags: {:?}. Intersection tags: {:?}.",
                            set, intersection
                        );
                        println!("{:?}", x)
                    }
                    obj_count[2] += 1;
                }
            }
        }
    }

    res_tx
        .send((
            diff / (&mut obj_count.clone()).iter().sum::<u64>() as f64,
            empty_count,
            obj_count,
            processed_node_tagged_count,
            processed_node_empty_count,
        ))
        .unwrap();
}

fn main() {
    let cpus = num_cpus::get();

    for arg in args().skip(1) {
        let mut workers = Vec::with_capacity(cpus);
        for _ in 0..cpus {
            let (req_tx, req_rx) = channel();
            let (res_tx, res_rx) = channel();
            workers.push((req_tx, res_rx));
            thread::spawn(move || {
                blobs_worker(req_rx, res_tx);
            });
        }

        println!("Open {}", arg);
        let f = File::open(&arg).unwrap();
        let mut reader = BlobReader::new(BufReader::new(f));
        let start = Instant::now();

        let mut w = 0;
        for blob in &mut reader {
            let req_tx = &workers[w].0;
            w = (w + 1) % cpus;

            req_tx.send(blob).unwrap();
        }
        let mut diff = 0.0_f64;
        let mut empty: [u64; 3] = [0; 3];
        let mut obj_counts: [u64; 3] = [0; 3];
        let mut processed_node_tagged_count: u64 = 0;
        let mut processed_node_empty_count: u64 = 0;
        for (req_tx, res_rx) in workers.into_iter() {
            drop(req_tx);
            let worker_diff = res_rx.recv().unwrap();
            diff += worker_diff.0;
            empty[0] += worker_diff.1[0];
            empty[1] += worker_diff.1[1];
            empty[2] += worker_diff.1[2];
            obj_counts[0] += worker_diff.2[0];
            obj_counts[1] += worker_diff.2[1];
            obj_counts[2] += worker_diff.2[2];
            processed_node_tagged_count += worker_diff.3;
            processed_node_empty_count += worker_diff.4;
        }
        diff = diff / cpus as f64;

        let stop = Instant::now();
        let duration = stop.duration_since(start);
        let duration = duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1e9);
        let mut f = reader.into_inner();
        match f.seek(SeekFrom::Current(0)) {
            Ok(pos) => {
                let rate = pos as f64 / 1024.0 / 1024.0 / duration;
                println!(
                    "Processed {} MB in {:.2} seconds ({:.2} MB/s)",
                    pos / 1024 / 1024,
                    duration,
                    rate
                );
            }
            Err(_) => (),
        }

        let empty_to_tags_ratio: [f64; 3] = [
            empty[0] as f64 / obj_counts[0] as f64,
            empty[1] as f64 / obj_counts[1] as f64,
            empty[2] as f64 / obj_counts[2] as f64,
        ];

        println!(
            "{} - Avg intersection diff: {}. Empty: {:?}. Num empty: {}. Obj counts: {:?}. Num objs: {}. Empty obj ratios: {:?}. Test processed_node_tagged_count: {}. Test processed_node_empty_count: {}.",
            arg,
            diff,
            empty,
            empty.iter().sum::<u64>(),
            obj_counts,
            obj_counts.iter().sum::<u64>(),
            empty_to_tags_ratio,
            processed_node_tagged_count,
            processed_node_empty_count,
        );
    }
}
