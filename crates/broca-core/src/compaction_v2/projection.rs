//! The production transcript projection materializes neutral entries once.
//! Required written_at values stay solely in the native message-time column.

use cortexkit_role_compaction::read::Cursor;

use super::metadata::{DeliveryUpdate, Fold};

pub struct Prepared {
    pub continuation: Fold,
    pub newest: Option<Cursor>,
    pub full: Vec<Option<String>>,
    pub compact: Vec<Option<String>>,
    pub completed: Vec<bool>,
    pub tags: Vec<Option<String>>,
    pub deliveries: Vec<DeliveryUpdate>,
}
