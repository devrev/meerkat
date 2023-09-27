import { cubeToDuckdbAST } from '@devrev/meerkat-core';
export function meerkatBrowser(): string {
  console.info(cubeToDuckdbAST);
  return 'meerkat-browser';
}
