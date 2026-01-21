export type GroupedChildren<T> = Array<GroupedNode<T>> | T[];

export type GroupedNode<T> = Partial<T> & {
  [key in `${Extract<keyof T, string>}_children`]?: GroupedChildren<T>;
};

function isEmptyValue(value: unknown): value is null | undefined {
  return value === null || value === undefined;
}

function valueToKey(value: unknown): string {
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

function summarizeGroupValues<T extends Record<string, unknown>>(records: T[]): Partial<T> {
  const uniqueValues = new Map<keyof T, Map<string, unknown>>();
  const keys = new Set<keyof T>();

  for (const record of records) {
    for (const key of Object.keys(record) as Array<keyof T>) {
      keys.add(key);
      const value = record[key];
      if (isEmptyValue(value)) {
        continue;
      }

      const fieldValues = uniqueValues.get(key) ?? new Map<string, unknown>();
      fieldValues.set(valueToKey(value), value);
      uniqueValues.set(key, fieldValues);
    }
  }

  const summary: Partial<T> = {};

  for (const key of keys) {
    const fieldValues = uniqueValues.get(key);
    if (!fieldValues || fieldValues.size === 0) {
      continue;
    }

    if (fieldValues.size === 1) {
      summary[key] = fieldValues.values().next().value as T[keyof T];
    } else {
      summary[key] = `${fieldValues.size} values` as T[keyof T];
    }
  }

  return summary;
}

export function groupByNested<
  T extends Record<string, unknown>,
  K extends Extract<keyof T, string>,
>(items: T[], groupings: K[]): GroupedNode<T>[] {
  if (groupings.length === 0) {
    return items as GroupedNode<T>[];
  }

  const [current, ...rest] = groupings;
  const groups = new Map<T[K], T[]>();

  for (const item of items) {
    const key = item[current];
    const existing = groups.get(key);
    if (existing) {
      existing.push(item);
    } else {
      groups.set(key, [item]);
    }
  }

  const childrenKey = `${current}_children` as `${K}_children`;

  return Array.from(groups.entries()).map(([groupValue, records]) => {
    const summary = summarizeGroupValues(records);
    const node = {
      ...summary,
      [current]: groupValue,
      [childrenKey]: rest.length > 0 ? groupByNested(records, rest) : records,
    } as GroupedNode<T>;

    return node;
  });
}
