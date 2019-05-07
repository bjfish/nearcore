use near_chain::RuntimeAdapter;
use near_primitives::types::AuthorityId;
use std::sync::Arc;

pub trait BaseOrchestrator: Send + Sync {
    fn is_shard_chunk_producer(
        &self,
        authority_id: AuthorityId,
        shard_id: u64,
        height: u64,
    ) -> bool;
    fn is_block_producer(&self, authority_id: AuthorityId, height: u64) -> bool;

    fn get_authority_id_for_part(&self, part_id: usize) -> AuthorityId;

    fn get_total_chunk_parts_num(&self) -> usize;
    fn get_data_chunk_parts_num(&self) -> usize;

    fn get_num_shards(&self) -> usize;
}

pub struct RuntimeAdapterOrchestrator {
    pub runtime_adapter: Arc<RuntimeAdapter>,
}

impl BaseOrchestrator for RuntimeAdapterOrchestrator {
    fn is_shard_chunk_producer(&self, authority_id: usize, shard_id: u64, height: u64) -> bool {
        unimplemented!()
    }

    fn is_block_producer(&self, authority_id: usize, height: u64) -> bool {
        unimplemented!()
    }

    fn get_authority_id_for_part(&self, part_id: usize) -> usize {
        unimplemented!()
    }

    fn get_total_chunk_parts_num(&self) -> usize {
        unimplemented!()
    }

    fn get_data_chunk_parts_num(&self) -> usize {
        unimplemented!()
    }

    fn get_num_shards(&self) -> usize {
        unimplemented!()
    }
}
