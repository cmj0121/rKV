.PHONY: all clean test fuzz run build bench bench-server fuzz-server docker-up docker-down cluster-up cluster-down upgrade help $(SUBDIR)

all: $(SUBDIR) 		# default action
	@[ -f .git/hooks/pre-commit ] || pre-commit install --install-hooks
	@git config commit.template .git-commit-template

clean: $(SUBDIR)	# clean-up environment
	@find . -name '*.sw[po]' -delete
	cargo clean

test:				# run all tests (all features)
	cargo test --workspace

fuzz:				# run fuzz test (RKV_FUZZ_SECS=N, RKV_FUZZ_SEED=N)
	RKV_FUZZ_SECS=60 cargo test -p rkv --test fuzz -- --nocapture

run:				# run in the local environment
	cargo run -p rkv

build:				# build the binary/library (all features)
	cargo build --workspace --release
	cargo build -p rkv --features server --release

bench:				# run benchmarks
	cargo run --bin bench --release

bench-server:			# run HTTP server benchmarks (no file output)
	cargo run --features server --bin bench_server --release -- --no-save

fuzz-server:			# run HTTP server fuzz test (RKV_SERVER_FUZZ_SECS=N)
	RKV_SERVER_FUZZ_SECS=60 cargo test -p rkv --features server --lib -- server::tests::fuzz_http_ops --nocapture

docker-up:			# start rKV in Docker (build + run)
	docker compose up --build -d

docker-down:			# stop rKV Docker service
	docker compose down

cluster-up:			# start rKV cluster (2 shard groups + gateway)
	docker compose --profile cluster up --build -d

cluster-down:			# stop rKV cluster
	docker compose --profile cluster down

upgrade:			# upgrade all the necessary packages
	pre-commit autoupdate

help:				# show this message
	@printf "Usage: make [OPTION]\n"
	@printf "\n"
	@perl -nle 'print $$& if m{^[\w-]+:.*?#.*$$}' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?#"} {printf "    %-18s %s\n", $$1, $$2}'
