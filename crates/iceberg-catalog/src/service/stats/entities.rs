use crate::WarehouseIdent;
use uuid::Uuid;

#[derive(Debug)]
pub struct WarehouseStatistics {
    pub warehouse_ident: WarehouseIdent,
    pub statistics_id: Uuid,
    pub number_of_tables: i64, // silly but necessary due to sqlx wanting i64, not usize
    pub number_of_views: i64,
}
