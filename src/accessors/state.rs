use crate::{
    crypto::keccak256,
    kv::{tables, traits::*},
    models::*,
    u256_to_h256,
};
use std::{fmt::Debug, hash::Hash};

pub trait StateStorageKind: Debug + Send + Sync + 'static
// https://github.com/rust-lang/rust/issues/20671
where
    tables::BitmapKey<Self::Address>: TableObject,
    tables::BitmapKey<(Self::Address, Self::Location)>: TableObject,
{
    const HASHED: bool;

    type Address: Copy + Debug + PartialEq + Hash + Ord + TableObject;
    type Location: Copy + Debug + PartialEq + Hash + Ord + TableObject;

    type AccountTable: Table<Key = Self::Address, Value = Account>;
    type StorageTable: Table<Key = Self::Address, Value = (Self::Location, U256)>
        + DupSort<SeekBothKey = Self::Location>;

    type AccountChangeTable: Table<Key = tables::AccountChangeKey, Value = tables::AccountChange<Self::Address>>
        + DupSort<SeekBothKey = Self::Address>;
    type StorageChangeTable: Table<
            Key = tables::StorageChangeKey<Self::Address>,
            Value = tables::StorageChange<Self::Location>,
        > + DupSort<SeekBothKey = Self::Location>;

    type AccountHistoryIndexTable: Table<
        Key = tables::BitmapKey<Self::Address>,
        Value = RoaringTreemap,
        SeekKey = tables::BitmapKey<Self::Address>,
    >;
    type StorageHistoryIndexTable: Table<
        Key = tables::BitmapKey<(Self::Address, Self::Location)>,
        Value = RoaringTreemap,
        SeekKey = tables::BitmapKey<(Self::Address, Self::Location)>,
    >;

    fn address_to_key(address: Address) -> Self::Address;
    fn location_to_key(location: U256) -> Self::Location;

    fn account_table() -> Self::AccountTable;
    fn storage_table() -> Self::StorageTable;

    fn account_change_table() -> Self::AccountChangeTable;
    fn storage_change_table() -> Self::StorageChangeTable;

    fn account_history_index_table() -> Self::AccountHistoryIndexTable;
    fn storage_history_index_table() -> Self::StorageHistoryIndexTable;
}

#[derive(Debug)]
pub struct PlainState;

impl StateStorageKind for PlainState {
    const HASHED: bool = false;

    type Address = Address;
    type Location = H256;

    type AccountTable = tables::Account;
    type StorageTable = tables::Storage;

    type AccountChangeTable = tables::AccountChangeSet;
    type StorageChangeTable = tables::StorageChangeSet;

    type AccountHistoryIndexTable = tables::AccountHistory;
    type StorageHistoryIndexTable = tables::StorageHistory;

    fn address_to_key(address: Address) -> Self::Address {
        address
    }
    fn location_to_key(location: U256) -> Self::Location {
        u256_to_h256(location)
    }

    fn account_table() -> Self::AccountTable {
        tables::Account
    }
    fn storage_table() -> Self::StorageTable {
        tables::Storage
    }

    fn account_change_table() -> Self::AccountChangeTable {
        tables::AccountChangeSet
    }
    fn storage_change_table() -> Self::StorageChangeTable {
        tables::StorageChangeSet
    }

    fn account_history_index_table() -> Self::AccountHistoryIndexTable {
        tables::AccountHistory
    }
    fn storage_history_index_table() -> Self::StorageHistoryIndexTable {
        tables::StorageHistory
    }
}

#[derive(Debug)]
pub struct HashedState;

impl StateStorageKind for HashedState {
    const HASHED: bool = true;

    type Address = H256;
    type Location = H256;

    type AccountTable = tables::HashedAccount;
    type StorageTable = tables::HashedStorage;

    type AccountChangeTable = tables::HashedAccountChangeSet;
    type StorageChangeTable = tables::HashedStorageChangeSet;

    type AccountHistoryIndexTable = tables::HashedAccountHistory;
    type StorageHistoryIndexTable = tables::HashedStorageHistory;

    fn address_to_key(address: Address) -> Self::Address {
        keccak256(address)
    }
    fn location_to_key(location: U256) -> Self::Location {
        keccak256(u256_to_h256(location))
    }

    fn account_table() -> Self::AccountTable {
        tables::HashedAccount
    }
    fn storage_table() -> Self::StorageTable {
        tables::HashedStorage
    }

    fn account_change_table() -> Self::AccountChangeTable {
        tables::HashedAccountChangeSet
    }
    fn storage_change_table() -> Self::StorageChangeTable {
        tables::HashedStorageChangeSet
    }

    fn account_history_index_table() -> Self::AccountHistoryIndexTable {
        tables::HashedAccountHistory
    }
    fn storage_history_index_table() -> Self::StorageHistoryIndexTable {
        tables::HashedStorageHistory
    }
}

pub mod account {
    use super::*;

    pub async fn read<'db, Tx, S>(
        tx: &Tx,
        address_to_find: Address,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<Option<Account>>
    where
        Tx: Transaction<'db>,
        S: StateStorageKind,
        tables::BitmapKey<S::Address>: TableObject,
        tables::BitmapKey<(S::Address, S::Location)>: TableObject,
    {
        let address_to_find = S::address_to_key(address_to_find);
        if let Some(block_number) = block_number {
            if let Some(block_number) = super::history_index::find_next_block(
                tx,
                S::account_history_index_table(),
                address_to_find,
                block_number,
            )
            .await?
            {
                if let Some(tables::AccountChange { address, account }) = tx
                    .cursor_dup_sort(S::account_change_table())
                    .await?
                    .seek_both_range(block_number, address_to_find)
                    .await?
                {
                    if address == address_to_find {
                        return Ok(account);
                    }
                }
            }
        }

        tx.get(S::account_table(), address_to_find).await
    }
}

pub mod storage {
    use super::*;

    pub async fn read<'db, Tx, S>(
        tx: &Tx,
        address: Address,
        location_to_find: U256,
        block_number: Option<BlockNumber>,
    ) -> anyhow::Result<U256>
    where
        Tx: Transaction<'db>,
        S: StateStorageKind,
        tables::BitmapKey<S::Address>: TableObject,
        tables::BitmapKey<(S::Address, S::Location)>: TableObject,
    {
        let address = S::address_to_key(address);
        let location_to_find = S::location_to_key(location_to_find);
        if let Some(block_number) = block_number {
            if let Some(block_number) = super::history_index::find_next_block(
                tx,
                S::storage_history_index_table(),
                (address, location_to_find),
                block_number,
            )
            .await?
            {
                if let Some(tables::StorageChange { location, value }) = tx
                    .cursor_dup_sort(S::storage_change_table())
                    .await?
                    .seek_both_range(
                        tables::StorageChangeKey {
                            block_number,
                            address,
                        },
                        location_to_find,
                    )
                    .await?
                {
                    if location == location_to_find {
                        return Ok(value);
                    }
                }
            }
        }

        Ok(tx
            .cursor_dup_sort(S::storage_table())
            .await?
            .seek_both_range(address, location_to_find)
            .await?
            .filter(|&(l, _)| l == location_to_find)
            .map(|(_, v)| v)
            .unwrap_or_default())
    }
}

pub mod history_index {
    use super::*;
    use crate::kv::tables::BitmapKey;
    use croaring::Treemap as RoaringTreemap;

    pub async fn find_next_block<'db: 'tx, 'tx, Tx: Transaction<'db>, K, H>(
        tx: &'tx Tx,
        table: H,
        needle: K,
        block_number: BlockNumber,
    ) -> anyhow::Result<Option<BlockNumber>>
    where
        H: Table<Key = BitmapKey<K>, Value = RoaringTreemap, SeekKey = BitmapKey<K>>,
        BitmapKey<K>: TableObject,
        K: Copy + PartialEq,
    {
        let mut ch = tx.cursor(table).await?;
        if let Some((index_key, change_blocks)) = ch
            .seek(BitmapKey {
                inner: needle,
                block_number,
            })
            .await?
        {
            if index_key.inner == needle {
                return Ok(change_blocks
                    .iter()
                    .find(|&change_block| *block_number < change_block)
                    .map(BlockNumber));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{h256_to_u256, kv::new_mem_database};
    use hex_literal::hex;

    async fn read_storage<S>()
    where
        S: StateStorageKind,
        tables::BitmapKey<S::Address>: TableObject,
        tables::BitmapKey<(S::Address, S::Location)>: TableObject,
    {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address = hex!("b000000000000000000000000000000000000008").into();

        let loc1 = h256_to_u256(H256(hex!(
            "000000000000000000000000000000000000a000000000000000000000000037"
        )));
        let loc2 = h256_to_u256(H256(hex!(
            "0000000000000000000000000000000000000000000000000000000000000000"
        )));
        let loc3 = h256_to_u256(H256(hex!(
            "ff00000000000000000000000000000000000000000000000000000000000017"
        )));
        let loc4 = h256_to_u256(H256(hex!(
            "00000000000000000000000000000000000000000000000000000000000f3128"
        )));

        let val1 = 0xc9b131a4_u128.into();
        let val2 = 0x5666856076ebaf477f07_u128.into();
        let val3 = h256_to_u256(H256(hex!(
            "4400000000000000000000000000000000000000000000000000000000000000"
        )));

        txn.set(
            S::storage_table(),
            S::address_to_key(address),
            (S::location_to_key(loc1), val1),
        )
        .await
        .unwrap();
        txn.set(
            S::storage_table(),
            S::address_to_key(address),
            (S::location_to_key(loc2), val2),
        )
        .await
        .unwrap();
        txn.set(
            S::storage_table(),
            S::address_to_key(address),
            (S::location_to_key(loc3), val3),
        )
        .await
        .unwrap();

        assert_eq!(
            super::storage::read::<_, S>(&txn, address, loc1, None)
                .await
                .unwrap(),
            val1
        );
        assert_eq!(
            super::storage::read::<_, S>(&txn, address, loc2, None)
                .await
                .unwrap(),
            val2
        );
        assert_eq!(
            super::storage::read::<_, S>(&txn, address, loc3, None)
                .await
                .unwrap(),
            val3
        );
        assert_eq!(
            super::storage::read::<_, S>(&txn, address, loc4, None)
                .await
                .unwrap(),
            0.as_u256()
        );
    }

    #[tokio::test]
    async fn read_plain_storage() {
        read_storage::<PlainState>().await
    }

    #[tokio::test]
    async fn read_hashed_storage() {
        read_storage::<HashedState>().await
    }
}
