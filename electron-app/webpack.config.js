const { composePlugins, withNx } = require('@nx/webpack');

// Nx plugins for webpack.
module.exports = composePlugins(withNx(), (config) => {
  // Update the webpack config as needed here.
  config.module = config.module || {};
  config.module.rules = config.module.rules || [];

  // Add rule for .node files with additional configuration
  config.module.rules.push({
    test: /\.node$/,
    loader: 'node-loader',
    options: {
      name: '[name].[ext]',
    },
  });

  // Enable native addons
  config.externals = {
    ...config.externals,
    '@duckdb/node-bindings-darwin-arm64':
      'commonjs2 @duckdb/node-bindings-darwin-arm64',
  };

  return config;
});
