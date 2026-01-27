.PHONY: antlr generate-dsl test dev

# ANTLR JAR location - download if not present
ANTLR_JAR ?= antlr-4.13.0-complete.jar
ANTLR_URL = https://www.antlr.org/download/$(ANTLR_JAR)

# DSL grammar and output directories
DSL_GRAMMAR = aerospike_fluent/dsl/antlr4/Condition.g4
DSL_OUTPUT = aerospike_fluent/dsl/antlr4/generated
DSL_GENERATED = $(DSL_OUTPUT)/ConditionLexer.py $(DSL_OUTPUT)/ConditionParser.py $(DSL_OUTPUT)/ConditionListener.py $(DSL_OUTPUT)/ConditionVisitor.py

antlr-download:
	@if [ ! -f $(ANTLR_JAR) ]; then \
		echo "Downloading ANTLR JAR..."; \
		curl -L -o $(ANTLR_JAR) $(ANTLR_URL); \
	fi

generate-dsl: antlr-download
	@echo "Checking Java version (requires Java 11+)..."
	@java -version 2>&1 | head -1 || (echo "Error: Java is not installed or not in PATH. ANTLR requires Java 11 or higher." && exit 1)
	@echo "Generating Python parser from ANTLR grammar..."
	@mkdir -p $(DSL_OUTPUT)
	@cd aerospike_fluent/dsl/antlr4 && java -jar ../../../$(ANTLR_JAR) -Dlanguage=Python3 -o generated -visitor -listener Condition.g4
	@touch $(DSL_OUTPUT)/__init__.py
	@echo "Generated parser files in $(DSL_OUTPUT)/"

clean-dsl:
	@echo "Cleaning generated DSL parser files..."
	@rm -rf $(DSL_OUTPUT)
	@echo "Cleaned DSL parser files"

dev:
	pip install -e ".[dev]"

test:
	pytest
