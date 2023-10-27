# Meerkat

Meerkat is TypeScript SDK solution that seamlessly converts Cube-like queries to DuckDB queries. Whether you're using a browser or NodeJS, Meerkat delivers a powerful layer of abstraction for executing advanced queries.

A major feature of Meerkat is its utilization of DuckDB's Abstract Syntax Tree (AST) and the json_serialize_sql native utility. This is directly employed for two critical operations: serializing and deserializing SQL to JSON. By using this, we can effectively utilize all the features of the DuckDB query language without the need to rely on another query builder like React-query builder.

The DuckDB JSON AST gives Meerkat an edge; its extensibility far outpaces traditional query builders, hence providing more powerful and flexible solutions to complex queries.

## Repository Structure

This repository is a monorepo managed using [Nx](https://nx.dev). It contains the following projects:

1. `meerkat-core` - The core library that contains the core functionality of Meerkat.
2. `meerkat-node` - The library uses `meerkat-core` and provides a NodeJS interface for executing queries.
3. `meerkat-browser` - The library uses `meerkat-core` and provides a browser interface for executing queries.

### 1. Environment Setup

1. Install NodeJS v18
1. From NodeJS website: [https://nodejs.org/](https://nodejs.org/)
1. or using ASDF: https://asdf-vm.com/
   1. `asdf plugin add nodejs`
   2. `asdf install`
   3. `node -v` Should print v18.x.y as defined in `.tool-versions` file
1. or using NVM: https://github.com/nvm-sh/nvm
   1. `nvm install 18`
   2. `nvm alias default 18`
   3. `nvm use default`
   4. `node -v` Should print v18.x.y
   5. Setup `.npmrc` file - Refer to [this](https://www.notion.so/devrev/Creation-of-npmrc-file-e0e423edce934218adfe538c105cf7fb) doc for more info.

## Installation

- `npm ci`

## Running SDK 

There is no application to run, for testing your functionality you need to run the tests.

Example of running all tests 
`npx nx run-many --targets tests --all`

