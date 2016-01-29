example: node_modules
	@$</.bin/future-node $@.js

node_modules: package.json
	@npm install

PHONY: example
