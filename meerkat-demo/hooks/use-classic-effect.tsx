import React from 'react';

export const useClassicEffect = createClassicEffectHook();

function createClassicEffectHook() {
  return (effect: React.EffectCallback, deps?: React.DependencyList) => {
    React.useEffect(() => {
      let isMounted = true;
      let unmount: void | (() => void);

      queueMicrotask(() => {
        if (isMounted) unmount = effect();
      });

      return () => {
        isMounted = false;
        unmount?.();
      };
    }, deps);
  };
}
