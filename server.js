const express = require('express');
const path = require('path');

const app = express();
const PORT = 4200;
const IFRAME_ORIGIN = 'http://localhost:4205';

// Set headers for SharedArrayBuffer and allow communication with iframe
app.use((req, res, next) => {
  // Set Cross-Origin-Opener-Policy
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');

  // Set Cross-Origin-Embedder-Policy
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');

  // Set Cross-Origin-Resource-Policy
  // Use 'same-site' if your main app and resources are on the same site
  // Use 'cross-origin' if resources are served from a different origin
  res.setHeader('Cross-Origin-Resource-Policy', 'same-site');

  // Allow the iframe origin
  res.setHeader(
    'Content-Security-Policy',
    `frame-ancestors 'self' ${IFRAME_ORIGIN}`
  );

  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', IFRAME_ORIGIN);
  res.setHeader(
    'Access-Control-Allow-Methods',
    'GET, POST, OPTIONS, PUT, PATCH, DELETE'
  );
  res.setHeader(
    'Access-Control-Allow-Headers',
    'X-Requested-With,content-type'
  );
  res.setHeader('Access-Control-Allow-Credentials', true);

  next();
});

// Serve static files from the React app build directory
app.use(
  express.static(path.join(__dirname, 'dist/benchmarking/benchmarking-app'))
);

// The "catch-all" handler: for any request that doesn't match one above, send back the index.html file.
app.get('*', (req, res) => {
  res.sendFile(
    path.join(__dirname, 'dist/benchmarking/benchmarking-app', 'index.html')
  );
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  console.log(`Allowed iframe origin: ${IFRAME_ORIGIN}`);
});
