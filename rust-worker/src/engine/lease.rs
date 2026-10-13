//! PostgreSQL shard lease acquisition, renewal, and release.
use anyhow::Result;
use sqlx::{PgPool, Row};

pub(crate) async fn acquire_lease(pool: &PgPool, shard: u32, owner: &str, ttl_ms: i64) -> Result<Option<i64>> {
    let row = sqlx::query(
        "INSERT INTO shard_leases (virtual_shard,owner,fencing_token,expires_at,routing_epoch) \
         VALUES ($1,$2,1,NOW()+($3 * INTERVAL '1 millisecond'),0) \
         ON CONFLICT (virtual_shard) DO UPDATE SET \
           owner=CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.owner ELSE shard_leases.owner END, \
           fencing_token=CASE WHEN shard_leases.expires_at < NOW() THEN shard_leases.fencing_token+1 ELSE shard_leases.fencing_token END, \
           expires_at=CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.expires_at ELSE shard_leases.expires_at END \
         RETURNING owner,fencing_token,expires_at > NOW() AS active"
    ).bind(i32::try_from(shard)?).bind(owner).bind(ttl_ms).fetch_one(pool).await?;
    let lease_owner: String = row.try_get("owner")?;
    let token: i64 = row.try_get("fencing_token")?;
    let active: bool = row.try_get("active")?;
    Ok((lease_owner == owner && active).then_some(token))
}

pub(crate) async fn renew_lease(pool: &PgPool, shard: u32, owner: &str, token: i64, ttl_ms: i64) -> Result<bool> {
    let affected = sqlx::query(
        "UPDATE shard_leases SET expires_at=NOW()+($4 * INTERVAL '1 millisecond') \
         WHERE virtual_shard=$1 AND owner=$2 AND fencing_token=$3 AND expires_at>NOW()"
    ).bind(i32::try_from(shard)?).bind(owner).bind(token).bind(ttl_ms)
     .execute(pool).await?.rows_affected();
    Ok(affected == 1)
}

pub(crate) async fn release_lease(pool: &PgPool, shard: u32, owner: &str) -> Result<()> {
    sqlx::query("UPDATE shard_leases SET expires_at=NOW()-INTERVAL '1 millisecond' WHERE virtual_shard=$1 AND owner=$2")
        .bind(i32::try_from(shard)?).bind(owner).execute(pool).await?;
    Ok(())
}
