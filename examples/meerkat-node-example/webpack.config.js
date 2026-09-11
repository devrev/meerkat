const { NxAppWebpackPlugin } = require('@nx/webpack/app-plugin');

module.exports = {
  plugins: [
    new NxAppWebpackPlugin({
      target: 'node',
      compiler: 'tsc',
      outputPath: '../../dist/examples/meerkat-node-example',
      main: './src/main.ts',
      tsConfig: './tsconfig.app.json',
      assets: ['./src/assets'],
    }),
  ],
};
