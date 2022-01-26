use crate::{
    kv::{tables, traits::*},
    models::*,
    u256_to_h256,
};

pub async fn seek_storage_key<'tx, C: CursorDupSort<'tx, tables::Storage>>(
    cur: &mut C,
    address: Address,
    location: U256,
) -> anyhow::Result<Option<U256>> {
    let location = u256_to_h256(location);
    if let Some((l, v)) = cur.seek_both_range(address, location).await? {
        if l == location {
            return Ok(Some(v));
        }
    }

    Ok(None)
}

pub async fn upsert_storage_value<'tx, C>(
    cur: &mut C,
    address: Address,
    location: U256,
    value: U256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, tables::Storage>,
{
    if seek_storage_key(cur, address, location).await?.is_some() {
        cur.delete_current().await?;
    }

    if value != 0 {
        cur.upsert(address, (u256_to_h256(location), value)).await?;
    }

    Ok(())
}

pub async fn seek_hashed_storage_key<'tx, C: CursorDupSort<'tx, tables::HashedStorage>>(
    cur: &mut C,
    hashed_address: H256,
    hashed_location: H256,
) -> anyhow::Result<Option<U256>> {
    Ok(cur
        .seek_both_range(hashed_address, hashed_location)
        .await?
        .filter(|&(l, _)| l == hashed_location)
        .map(|(_, v)| v))
}

pub async fn upsert_hashed_storage_value<'tx, C>(
    cur: &mut C,
    hashed_address: H256,
    hashed_location: H256,
    value: U256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, tables::HashedStorage>,
{
    if seek_hashed_storage_key(cur, hashed_address, hashed_location)
        .await?
        .is_some()
    {
        cur.delete_current().await?;
    }

    if value != 0 {
        cur.upsert(hashed_address, (hashed_location, value)).await?;
    }

    Ok(())
}
