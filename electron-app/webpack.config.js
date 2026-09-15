const path = require('path');

const { NxAppWebpackPlugin } = require('@nx/webpack/app-plugin');

const configuration = process.env.NX_TASK_TARGET_CONFIGURATION || 'default';
const production = configuration === 'production';

module.exports = {
  target: 'electron-main',
  externals: {
    '@duckdb/node-bindings-darwin-arm64':
      'commonjs2 @duckdb/node-bindings-darwin-arm64',
  },
  output: {
    filename: '[name].js',
  },
  module: {
    rules: [
      {
        test: /\.node$/,
        loader: 'node-loader',
        options: {
          name: '[name].[ext]',
        },
      },
    ],
  },
  plugins: [
    new NxAppWebpackPlugin({
      outputPath: '../dist/electron-app',
      main: './src/main.ts',
      tsConfig: './tsconfig.app.json',
      assets: ['./src/assets'],
      target: 'node',
      compiler: 'swc',
      generatePackageJson: true,
      optimization: production,
      extractLicenses: production,
      fileReplacements: production
        ? [
            {
              replace: './src/environments/environment.ts',
              with: './src/environments/environment.prod.ts',
            },
          ]
        : [],
    }),
    {
      apply(compiler) {
        compiler.options.entry = {
          ...compiler.options.entry,
          'main.preload': {
            import: [
              path.resolve(__dirname, 'src/app/api/main.preload.ts'),
            ],
          },
        };
      },
    },
  ],
};
