To run fuzz tests:
1. Get a nightly toolchain

    cd fuzz
    rustup override set nightly

2. Install cargo-fuzz

    cargo install cargo-fuzz


3. Make a place for the data to go

    export YAQUE_FUZZ_BASE_DIR=`mktemp -d yaque-fuzz-XXXX --tmpdir`
    echo "${YAQUE_FUZZ_BASE_DIR}"

4. Run the tests

    cargo fuzz run queue_operations
