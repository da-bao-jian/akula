use bytes::BytesMut;

use crate::models::*;

#[derive(Clone, Debug)]
struct Cell {
    h: [u8; 32],        // Cell hash
    hl: usize,          // Length of the hash (or embedded)
    apk: [u8; 20],      // account plain key
    apl: usize,         // length of account plain key
    spk: [u8; 20 + 32], // storage plain key
    spl: usize,         // length of the storage plain key
    down_hashed_key: [u8; 128],
    down_hashed_len: usize,
    extension: [u8; 64],
    ext_len: usize,
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: H256, // hash of the bytecode
    pub storage: H256,
    pub storage_len: usize,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            h: Default::default(),
            hl: Default::default(),
            apk: Default::default(),
            apl: Default::default(),
            spk: [0; 52],
            spl: Default::default(),
            down_hashed_key: [0; 128],
            down_hashed_len: Default::default(),
            extension: [0; 64],
            ext_len: Default::default(),
            nonce: Default::default(),
            balance: Default::default(),
            code_hash: EMPTY_HASH,
            storage: Default::default(),
            storage_len: Default::default(),
        }
    }
}

/// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
/// with keys pre-hashed by keccak256
pub struct HexPatriciaHashed {
    root: Cell, // Root cell of the tree
    // Rows of the grid correspond to the level of depth in the patricia tree
    // Columns of the grid correspond to pointers to the nodes further from the root
    grid: [[Cell; 16]; 128], // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
    // How many rows (starting from row 0) are currently active and have corresponding selected columns
    // Last active row does not have selected column
    activeRows: usize,
    // Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
    // if a account leaf cell represents multiple nibbles in the key
    currentKeyLen: int,
    currentKey: [u8; 128], // For each row indicates which column is currently selected
    depths: [usize; 128],  // For each row, the depth of cells in that row
    rootChecked: bool, // Set to false if it is not known whether the root is empty, set to true if it is checked
    rootMod: bool,
    rootDel: bool,
    beforeBitmap: [u16; 128], // For each row, bitmap of cells that were present before modification
    modBitmap: [u16; 128],    // For each row, bitmap of cells that were modified (not deleted)
    delBitmap: [u16; 128],    // For each row, bitmap of cells that were deleted
    // Function used to load branch node and fill up the cells
    // For each cell, it sets the cell type, clears the modified flag, fills the hash,
    // and for the extension, account, and leaf type, the `l` and `k`
    // branchFn: Box<dyn Fn(prefix: &[u8]) -> func(prefix []byte) ([]byte, error)
    // Function used to fetch account with given plain key. It loads
    // accountFn func(plainKey []byte, cell *Cell) error
    // Function used to fetch account with given plain key
    // storageFn       func(plainKey []byte, cell *Cell) error
    // keccak          keccakState
    // keccak2         keccakState
    accountKeyLen: usize,
    trace: bool,
    numBuf: [u8; 10],
    byteArrayWriter: BytesMut,
    hashBuf: [u8; 33], // RLP representation of hash (or un-hashes value)
    keyPrefix: [u8; 1],
    lenPrefix: [u8; 4],
    valBuf: [u8; 128], // Enough to accommodate hash encoding of any account
    b: [u8; 1],        // Buffer for single byte
    prefixBuf: [u8; 8],
}
