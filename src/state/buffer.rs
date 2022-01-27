use crate::{
    accessors::{self, state::StateStorageKind},
    kv::{
        tables::{self, AccountChange, StorageChange, StorageChangeKey},
        traits::*,
    },
    models::*,
    state::database::*,
    State,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
};
use tokio_stream::StreamExt;
use tracing::*;

// address -> storage-encoded initial value
pub type AccountChanges = HashMap<Address, Option<Account>>;

#[derive(Debug)]
pub struct AddressStorageChanges<S>
where
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    erased_committed_storage: BTreeMap<S::Location, U256>,
    slots: BTreeMap<U256, U256>,
}

impl<S> Default for AddressStorageChanges<S>
where
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    fn default() -> Self {
        Self {
            erased_committed_storage: BTreeMap::new(),
            slots: Default::default(),
        }
    }
}

// address -> location -> zeroless initial value
pub type StorageChanges<S> = HashMap<Address, AddressStorageChanges<S>>;

#[derive(Default, Debug)]
struct OverlayStorage {
    erased: bool,
    slots: HashMap<U256, U256>,
}

#[derive(Debug)]
pub struct Buffer<'db, 'tx, Tx, S>
where
    'db: 'tx,
    Tx: Transaction<'db>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    txn: &'tx Tx,
    _marker: PhantomData<&'db ()>,

    prune_from: BlockNumber,
    historical_block: Option<BlockNumber>,

    accounts: HashMap<Address, Option<Account>>,

    // address -> location -> value
    storage: HashMap<Address, OverlayStorage>,

    account_changes: BTreeMap<BlockNumber, AccountChanges>, // per block
    storage_changes: BTreeMap<BlockNumber, StorageChanges<S>>, // per block

    hash_to_code: BTreeMap<H256, Bytes>,
    logs: BTreeMap<(BlockNumber, TxIndex), Vec<Log>>,

    // Current block stuff
    block_number: BlockNumber,
    changed_storage: HashSet<Address>,
}

impl<'db, 'tx, Tx, S> Buffer<'db, 'tx, Tx, S>
where
    'db: 'tx,
    Tx: Transaction<'db>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    pub fn new(
        txn: &'tx Tx,
        prune_from: BlockNumber,
        historical_block: Option<BlockNumber>,
    ) -> Self {
        Self {
            txn,
            prune_from,
            historical_block,
            _marker: PhantomData,
            accounts: Default::default(),
            storage: Default::default(),
            account_changes: Default::default(),
            storage_changes: Default::default(),
            hash_to_code: Default::default(),
            logs: Default::default(),
            block_number: Default::default(),
            changed_storage: Default::default(),
        }
    }

    pub fn insert_receipts(&mut self, block_number: BlockNumber, receipts: Vec<Receipt>) {
        for (i, receipt) in receipts.into_iter().enumerate() {
            self.logs
                .insert((block_number, TxIndex(i.try_into().unwrap())), receipt.logs);
        }
    }
}

#[async_trait]
impl<'db, 'tx, Tx, S> State for Buffer<'db, 'tx, Tx, S>
where
    'db: 'tx,
    Tx: Transaction<'db>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    async fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>> {
        if let Some(account) = self.accounts.get(&address) {
            return Ok(*account);
        }

        accessors::state::account::read::<_, S>(self.txn, address, self.historical_block).await
    }

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes> {
        if let Some(code) = self.hash_to_code.get(&code_hash).cloned() {
            Ok(code)
        } else {
            Ok(self
                .txn
                .get(tables::Code, code_hash)
                .await?
                .map(From::from)
                .unwrap_or_default())
        }
    }

    async fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256> {
        if let Some(account_storage) = self.storage.get(&address) {
            if let Some(value) = account_storage.slots.get(&location) {
                return Ok(*value);
            } else if account_storage.erased {
                return Ok(U256::ZERO);
            }
        }

        accessors::state::storage::read::<_, S>(self.txn, address, location, self.historical_block)
            .await
    }

    async fn erase_storage(&mut self, address: Address) -> anyhow::Result<()> {
        let mut mark_database_as_discarded = false;
        let overlay_storage = self.storage.entry(address).or_insert_with(|| {
            // If we don't have any overlay storage, we must mark slots in database as zeroed.
            mark_database_as_discarded = true;

            OverlayStorage {
                erased: true,
                slots: Default::default(),
            }
        });

        let storage_changes = self
            .storage_changes
            .entry(self.block_number)
            .or_default()
            .entry(address)
            .or_default();

        // Erase buffered storage
        for (slot, value) in overlay_storage.slots.drain() {
            storage_changes.slots.insert(slot, value);
        }

        if !overlay_storage.erased {
            // If we haven't erased before, we also must mark unmodified slots as zeroed.
            mark_database_as_discarded = true;
            overlay_storage.erased = true;
        }

        if mark_database_as_discarded {
            let storage_changes = self
                .storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default();

            let mut storage_table = self.txn.cursor_dup_sort(S::storage_table()).await?;

            storage_changes.erased_committed_storage =
                walk_dup(&mut storage_table, S::address_to_key(address))
                    .collect::<anyhow::Result<Vec<(S::Location, U256)>>>()
                    .await?
                    .into_iter()
                    .collect();
        }

        Ok(())
    }

    async fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        accessors::chain::header::read(self.txn, block_hash, block_number).await
    }

    async fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>> {
        accessors::chain::block_body::read_without_senders(self.txn, block_hash, block_number).await
    }

    async fn total_difficulty(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>> {
        accessors::chain::td::read(self.txn, block_hash, block_number).await
    }

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: BlockNumber) {
        self.block_number = block_number;
        self.changed_storage.clear();
    }

    fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) {
        let equal = current == initial;
        let account_deleted = current.is_none();

        if equal && !account_deleted && !self.changed_storage.contains(&address) {
            return;
        }

        if self.block_number >= self.prune_from {
            self.account_changes
                .entry(self.block_number)
                .or_default()
                .insert(address, initial);
        }

        if equal {
            return;
        }

        self.accounts.insert(address, current);
    }

    async fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()> {
        self.hash_to_code.insert(code_hash, code);

        Ok(())
    }

    async fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    ) -> anyhow::Result<()> {
        if current == initial {
            return Ok(());
        }

        if self.block_number >= self.prune_from {
            self.changed_storage.insert(address);
            self.storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default()
                .slots
                .insert(location, initial);
        }

        self.storage
            .entry(address)
            .or_default()
            .slots
            .insert(location, current);

        Ok(())
    }
}

impl<'db, 'tx, Tx, S> Buffer<'db, 'tx, Tx, S>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
    S: StateStorageKind,
    tables::BitmapKey<S::Address>: TableObject,
    tables::BitmapKey<(S::Address, S::Location)>: TableObject,
{
    pub async fn write_history(&mut self) -> anyhow::Result<()> {
        debug!("Writing account changes");
        let mut account_change_table = self
            .txn
            .mutable_cursor_dupsort(S::account_change_table())
            .await?;
        for (block_number, account_entries) in std::mem::take(&mut self.account_changes) {
            for (address, account) in account_entries
                .into_iter()
                .map(|(address, account)| (S::address_to_key(address), account))
                .collect::<BTreeMap<_, _>>()
            {
                account_change_table
                    .append_dup(block_number, AccountChange { address, account })
                    .await?;
            }
        }

        debug!("Writing storage changes");
        let mut storage_change_table = self
            .txn
            .mutable_cursor_dupsort(S::storage_change_table())
            .await?;
        for (block_number, storage_entries) in std::mem::take(&mut self.storage_changes) {
            for (address, storage_changes) in storage_entries
                .into_iter()
                .map(|(address, changes)| (S::address_to_key(address), changes))
                .collect::<BTreeMap<_, _>>()
            {
                let entries = storage_changes
                    .erased_committed_storage
                    .into_iter()
                    .chain(
                        storage_changes
                            .slots
                            .into_iter()
                            .map(|(location, value)| (S::location_to_key(location), value)),
                    )
                    .collect::<BTreeMap<_, _>>();
                for (location, value) in entries {
                    storage_change_table
                        .append_dup(
                            StorageChangeKey {
                                block_number,
                                address,
                            },
                            StorageChange { location, value },
                        )
                        .await?;
                }
            }
        }

        debug!("Writing logs");
        let mut log_table = self.txn.mutable_cursor(tables::Log).await?;
        for ((block_number, idx), logs) in std::mem::take(&mut self.logs) {
            log_table.append((block_number, idx), logs).await?;
        }

        debug!("History write complete");

        Ok(())
    }

    pub async fn write_to_db(mut self) -> anyhow::Result<()> {
        self.write_history().await?;

        // Write to state tables
        let mut account_table = self.txn.mutable_cursor(S::account_table()).await?;
        let mut storage_table = self.txn.mutable_cursor_dupsort(S::storage_table()).await?;

        debug!("Writing accounts");
        let mut written_accounts = 0;
        for (address_key, (address, account)) in self
            .accounts
            .into_iter()
            .map(|(address, account)| (S::address_to_key(address), (address, account)))
            .collect::<BTreeMap<_, _>>()
        {
            if let Some(account) = account {
                account_table.upsert(address_key, account).await?;
            } else if account_table.seek_exact(address_key).await?.is_some() {
                account_table.delete_current().await?;
            }

            written_accounts += 1;
            if written_accounts % 500_000 == 0 {
                debug!(
                    "Written {} updated acccounts, current entry: {}",
                    written_accounts, address
                )
            }
        }

        debug!("Writing {} accounts complete", written_accounts);

        debug!("Writing storage");
        let mut written_slots = 0;
        for (address_key, (address, overlay_storage)) in self
            .storage
            .into_iter()
            .map(|(address, overlay_storage)| {
                (S::address_to_key(address), (address, overlay_storage))
            })
            .collect::<BTreeMap<_, _>>()
        {
            if overlay_storage.erased && storage_table.seek_exact(address_key).await?.is_some() {
                storage_table.delete_current_duplicates().await?;
            }

            for (location, value) in overlay_storage
                .slots
                .into_iter()
                .map(|(location, value)| (S::location_to_key(location), value))
                .collect::<BTreeMap<_, _>>()
            {
                upsert_storage_value::<_, S>(&mut storage_table, address_key, location, value)
                    .await?;

                written_slots += 1;
                if written_slots % 500_000 == 0 {
                    debug!(
                        "Written {} storage slots, current entry: address {}, slot {}",
                        written_slots, address, location
                    );
                }
            }
        }

        debug!("Writing {} slots complete", written_slots);

        debug!("Writing code");
        let mut code_table = self.txn.mutable_cursor(tables::Code).await?;
        for (code_hash, code) in self.hash_to_code {
            code_table.upsert(code_hash, code).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accessors::state::{HashedState, PlainState},
        kv::new_mem_database,
    };
    use hex_literal::hex;

    async fn storage_update<S>()
    where
        S: StateStorageKind,
        tables::BitmapKey<S::Address>: TableObject,
        tables::BitmapKey<(S::Address, S::Location)>: TableObject,
    {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address: Address = hex!("be00000000000000000000000000000000000000").into();

        let location_a = 0x13.as_u256();
        let value_a1 = 0x6b.as_u256();
        let value_a2 = 0x85.as_u256();

        let location_b = 0x02.as_u256();
        let value_b = 0x132.as_u256();

        let mut storage_table = txn
            .mutable_cursor_dupsort(S::storage_table())
            .await
            .unwrap();
        upsert_storage_value::<_, S>(
            &mut storage_table,
            S::address_to_key(address),
            S::location_to_key(location_a),
            value_a1,
        )
        .await
        .unwrap();
        upsert_storage_value::<_, S>(
            &mut storage_table,
            S::address_to_key(address),
            S::location_to_key(location_b),
            value_b,
        )
        .await
        .unwrap();

        let mut buffer = Buffer::<_, S>::new(&txn, 0.into(), None);

        assert_eq!(
            buffer.read_storage(address, location_a).await.unwrap(),
            value_a1
        );

        // Update only location A
        buffer
            .update_storage(address, location_a, value_a1, value_a2)
            .await
            .unwrap();
        buffer.write_to_db().await.unwrap();

        // Location A should have the new value
        let db_value_a = seek_storage_key::<_, S>(
            &mut txn.cursor_dup_sort(S::storage_table()).await.unwrap(),
            S::address_to_key(address),
            S::location_to_key(location_a),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_a, value_a2);

        // Location B should not change
        let db_value_b = seek_storage_key::<_, S>(
            &mut txn.cursor_dup_sort(S::storage_table()).await.unwrap(),
            S::address_to_key(address),
            S::location_to_key(location_b),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_b, value_b);
    }

    #[tokio::test]
    async fn plain_storage_update() {
        storage_update::<PlainState>().await
    }

    #[tokio::test]
    async fn hashed_storage_update() {
        storage_update::<HashedState>().await
    }
}
