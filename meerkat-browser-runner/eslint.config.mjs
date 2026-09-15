import baseConfig, {
  reactHooksCompatibilityConfig,
} from '../eslint.config.mjs';
import nx from '@nx/eslint-plugin';

export default [
  ...baseConfig,
  ...nx.configs['flat/react'],
  reactHooksCompatibilityConfig,
];
