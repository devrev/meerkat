export const memberKeyToSafeKey = (memberKey: string) => {
  return memberKey.split('.').join('__');
};
