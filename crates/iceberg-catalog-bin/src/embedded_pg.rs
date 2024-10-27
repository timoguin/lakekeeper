use axum::routing::post;
use iceberg_catalog::CONFIG;
use postgresql_embedded::PostgreSQL;

pub(super) async fn start_embedded_pg(
) -> Result<postgresql_embedded::PostgreSQL, postgresql_embedded::Error> {
    let settings = CONFIG.embedded_pg_settings();
    let mut postgresql = PostgreSQL::new(settings);

    println!("Setting up embedded postgres");
    postgresql.setup().await?;
    println!("Starting embedded postgres");
    postgresql.start().await?;

    let db_name = CONFIG
        .pg_database
        .clone()
        .expect("Tried to launch embedded postgres but pg_database is not set");
    if !postgresql.database_exists(&db_name).await? {
        println!("Creating database {}", db_name);
        postgresql.create_database(&db_name).await?;
    } else {
        println!("Database {} already exists", db_name);
    }

    Ok(postgresql)
}
