use cached::SizedCache;
use wasmer_runtime;

use crate::prepare;
use crate::types::{Config, ContractCode, Error};
use primitives::hash::{hash, CryptoHash};
use primitives::serialize::Encode;
use std::sync::Arc;

/// Cache size in number of cached modules to hold.
const CACHE_SIZE: usize = 1024;
// TODO: store a larger on-disk cache

cached_key! {
    CODE: SizedCache<CryptoHash, Result<Arc<ContractCode>, String>> = SizedCache::with_size(CACHE_SIZE);
    Key = {
        code_hash
    };

    fn get_code_with_cache(code_hash: CryptoHash, f: impl FnOnce() -> Result<ContractCode, String>) -> Result<Arc<ContractCode>, String> = {
        let code = f()?;
        assert_eq!(code_hash, code.get_hash());
        Ok(Arc::new(code))
    }
}

cached_key! {
    MODULES: SizedCache<(CryptoHash, CryptoHash), Result<wasmer_runtime::Module, Error>> = SizedCache::with_size(CACHE_SIZE);
    Key = {
        (code.get_hash(), hash(&config.encode().expect("encoding of config shouldn't fail")))
    };

    fn compile_cached_module(code: &ContractCode, config: &Config) -> Result<wasmer_runtime::Module, Error> = {
        let prepared_code = prepare::prepare_contract(code, config).map_err(Error::Prepare)?;

        // Metering compilation
        // use wasmer_runtime_core::codegen::{MiddlewareChain, StreamingCompiler};
        // use wasmer_singlepass_backend::ModuleCodeGenerator as SinglePassMCG;
        // use wasmer_middleware_common::metering::Metering;
        // let limit = 1_000_000;
        // let c: StreamingCompiler<SinglePassMCG, _, _, _, _> = StreamingCompiler::new(move || {
        //     let mut chain = MiddlewareChain::new();
        //     chain.push(Metering::new(limit));
        //     chain
        // });            
        // wasmer_runtime_core::compile_with(&prepared_code, &c)
        // .map_err(|e| Error::Wasmer(format!("{}", e)))

        // Normal compilation
        wasmer_runtime::compile(&prepared_code)
            .map_err(|e| Error::Wasmer(format!("{}", e)))
    }
}
