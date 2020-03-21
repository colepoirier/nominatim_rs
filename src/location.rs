use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub struct InvalidLocationError(pub String);

impl Display for InvalidLocationError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl Error for InvalidLocationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

pub const COORDINATE_PRECISION: u64 = 10_000_000;

pub fn str_to_coord(data: String) {
    
}