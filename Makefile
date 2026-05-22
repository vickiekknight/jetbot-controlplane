# Convenience targets for reviewers.
# All targets assume `pip install -r requirements.txt` has been run.

.PHONY: help install test demo interactive bench clean

help:
	@echo "JetBot Control Plane — make targets"
	@echo ""
	@echo "  make install     install dependencies into the current Python env"
	@echo "  make test        run the full test suite"
	@echo "  make demo        run the fully-automated multi-robot demo"
	@echo "  make interactive boot the system and hand off for manual CLI use"
	@echo "  make bench       run the latency benchmark"
	@echo "  make clean       remove __pycache__, .pytest_cache, demo logs"

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

demo:
	python -m demo.automated

interactive:
	python -m demo.interactive

bench:
	python -m benchmarks.latency

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
	rm -rf /tmp/jetbot-demo