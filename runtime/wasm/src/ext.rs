use primitives::types::{AccountId, Balance, PromiseId};

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    NotImplemented,
    PromiseIdNotFound,
    WrongPromise,
    PromiseAlreadyHasCallback,
    TrieIteratorError,
    TrieIteratorMissing,
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub trait External {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_iter(&mut self, prefix: &[u8]) -> Result<u32>;

    fn storage_range(&mut self, start: &[u8], end: &[u8]) -> Result<u32>;

    fn storage_iter_next(&mut self, id: u32) -> Result<Option<Vec<u8>>>;

    fn storage_iter_peek(&mut self, id: u32) -> Result<Option<Vec<u8>>>;

    fn storage_iter_remove(&mut self, id: u32);

    fn promise_create(
        &mut self,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
    ) -> Result<PromiseId>;

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
    ) -> Result<PromiseId>;

    fn check_ethash(
        &mut self,
        block_number: u64,
        header_hash: &[u8],
        nonce: u64,
        mix_hash: &[u8],
        difficulty: u64,
    ) -> bool;
}
