module.exports = {
  appId: 'ai.devrev.meerkat',
  productName: 'Meerkat',
  npmRebuild: false,
  files: [
    'package.json',
    {
      from: 'dist/electron-app',
      to: 'electron-app',
      filter: ['main.js', 'main.preload.js', 'assets/**/*'],
    },
    {
      from: 'dist/benchmarking',
      to: 'benchmarking',
      filter: ['**/*', '!**/*.map'],
    },
  ],
  extraMetadata: {
    main: 'electron-app/main.js',
  },
};
