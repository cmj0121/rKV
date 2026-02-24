.PHONY: all clean test run build bench upgrade help $(SUBDIR)

all: $(SUBDIR) 		# default action
	@[ -f .git/hooks/pre-commit ] || pre-commit install --install-hooks
	@git config commit.template .git-commit-template

clean: $(SUBDIR)	# clean-up environment
	@find . -name '*.sw[po]' -delete
	cargo clean

test:				# run all tests (all features)
	cargo test --workspace

run:				# run in the local environment
	cargo run -p rkv

build:				# build the binary/library (all features)
	cargo build --workspace --release

bench:				# run benchmarks and update BENCH.md
	cargo run --bin bench --release

upgrade:			# upgrade all the necessary packages
	pre-commit autoupdate

help:				# show this message
	@printf "Usage: make [OPTION]\n"
	@printf "\n"
	@perl -nle 'print $$& if m{^[\w-]+:.*?#.*$$}' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?#"} {printf "    %-18s %s\n", $$1, $$2}'
