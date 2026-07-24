use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use regex::Regex;

use crate::memtable::{Labels, LogEntry};
use crate::part::ExactFieldPredicate;

pub const PARSER_ERROR_FIELD: &str = "__error__";

include!("field_filters.rs");
include!("ast.rs");
include!("parser.rs");
include!("pipeline.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
