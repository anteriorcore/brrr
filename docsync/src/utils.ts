const sentinelRegex = /<docsync>(.*?)<\/docsync>/;

export function mergeMaps<T, U>(maps: Map<T, U>[]): Map<T, U> {
  return new Map(maps.flatMap((x) => [...x]));
}

export function parseSentinel(docstring: string) {
  return docstring.match(sentinelRegex)?.at(1);
}
