use crate::{
    accessors::state::StateStorageKind,
    kv::{tables, traits::*},
    models::*,
};

pub async fn seek_storage_key<'tx, C, S>(
    cur: &mut C,
    address: S::Address,
    location: S::Location,
) -> anyhow::Result<Option<U256>>
where
    C: CursorDupSort<'tx, S::StorageTable>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    if let Some((l, v)) = cur.seek_both_range(address, location).await? {
        if l == location {
            return Ok(Some(v));
        }
    }

    Ok(None)
}

pub async fn upsert_storage_value<'tx, C, S>(
    cur: &mut C,
    address: S::Address,
    location: S::Location,
    value: U256,
) -> anyhow::Result<()>
where
    C: MutableCursorDupSort<'tx, S::StorageTable>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    if seek_storage_key::<_, S>(cur, address, location)
        .await?
        .is_some()
    {
        cur.delete_current().await?;
    }

    if value != 0 {
        cur.upsert(address, (location, value)).await?;
    }

    Ok(())
}
