import {
  Dimension,
  Measure,
  Query,
  TableSchema,
} from '../types/cube-types';

export type AliasableMember = Measure | Dimension;
type MemberType = 'measure' | 'dimension';

export interface EnsureAliasExpressionContext {
  tableName: string;
  memberName: string;
  memberType: MemberType;
  /**
   * Names of all tables in the current schema batch. Used to treat
   * `otherTable.col` as an intentional cross-table reference rather than a
   * struct access.
   */
  knownTableNames?: Set<string>;
}

export interface EnsureTableSchemaAliasSqlParams {
  tableSchemas: TableSchema[];
  /**
   * Query whose member references drive which table-schema members are
   * aliased. Members that the query does not reach are left untouched and
   * never sent through `ensureExpressionAlias` — this is the primary
   * optimization when tenants have thousands of dimensions.
   */
  query: Query;
  ensureExpressionAlias: (params: {
    items: {
      sql: string;
      context: EnsureAliasExpressionContext;
    }[];
  }) => Promise<string[]>;
}

interface AliasableMemberDescriptor {
  sql: string;
  context: EnsureAliasExpressionContext;
  apply: (aliasedSql: string) => void;
}

const collectFilterMembers = (filter: unknown, acc: Set<string>): void => {
  if (!filter || typeof filter !== 'object') {
    return;
  }
  const candidate = filter as {
    and?: unknown[];
    or?: unknown[];
    member?: string;
  };
  if (Array.isArray(candidate.and)) {
    candidate.and.forEach((child) => collectFilterMembers(child, acc));
    return;
  }
  if (Array.isArray(candidate.or)) {
    candidate.or.forEach((child) => collectFilterMembers(child, acc));
    return;
  }
  if (typeof candidate.member === 'string') {
    acc.add(candidate.member);
  }
};

const collectReferencedMembers = (query: Query): Set<string> => {
  const referenced = new Set<string>();

  query.measures.forEach((m) => referenced.add(m));
  query.dimensions?.forEach((d) => referenced.add(d));

  if (query.order) {
    Object.keys(query.order).forEach((m) => referenced.add(m));
  }

  query.filters?.forEach((filter) => collectFilterMembers(filter, referenced));

  return referenced;
};

const collectAliasableDescriptors = ({
  members,
  memberType,
  tableName,
  knownTableNames,
  referencedMembers,
  descriptors,
}: {
  members: AliasableMember[];
  memberType: MemberType;
  tableName: string;
  knownTableNames?: Set<string>;
  referencedMembers: Set<string>;
  descriptors: AliasableMemberDescriptor[];
}): void => {
  members.forEach((member) => {
    if (!referencedMembers.has(`${tableName}.${member.name}`)) {
      return;
    }
    descriptors.push({
      sql: member.sql,
      context: {
        tableName,
        memberName: member.name,
        memberType,
        knownTableNames,
      },
      apply: (aliasedSql: string) => {
        member.sql = aliasedSql;
      },
    });
  });
};

const ensureDescriptorBatchAlias = async ({
  descriptors,
  ensureExpressionAlias,
  tableName,
}: {
  descriptors: AliasableMemberDescriptor[];
  ensureExpressionAlias: EnsureTableSchemaAliasSqlParams['ensureExpressionAlias'];
  tableName: string;
}): Promise<void> => {
  if (descriptors.length === 0) {
    return;
  }

  const items = descriptors.map((descriptor) => ({
    sql: descriptor.sql,
    context: descriptor.context,
  }));

  try {
    const aliasedSqls = await ensureExpressionAlias({ items });

    if (aliasedSqls.length !== descriptors.length) {
      throw new Error(
        `Expected ${descriptors.length} aliased expressions, received ${aliasedSqls.length}`
      );
    }

    aliasedSqls.forEach((aliasedSql, index) => {
      descriptors[index].apply(aliasedSql);
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to ensure alias for table ${tableName}: ${message}`
    );
  }
};

export const ensureTableSchemaAliasSql = async ({
  tableSchemas,
  query,
  ensureExpressionAlias,
}: EnsureTableSchemaAliasSqlParams): Promise<TableSchema[]> => {
  const knownTableNames = new Set(tableSchemas.map((s) => s.name));
  const referencedMembers = collectReferencedMembers(query);

  return Promise.all(
    tableSchemas.map(async (tableSchema) => {
      const aliasedTableSchema: TableSchema = {
        ...tableSchema,
        measures: tableSchema.measures.map((measure) => ({ ...measure })),
        dimensions: tableSchema.dimensions.map((dimension) => ({
          ...dimension,
        })),
      };

      const descriptors: AliasableMemberDescriptor[] = [];
      collectAliasableDescriptors({
        members: aliasedTableSchema.measures,
        memberType: 'measure',
        tableName: tableSchema.name,
        knownTableNames,
        referencedMembers,
        descriptors,
      });
      collectAliasableDescriptors({
        members: aliasedTableSchema.dimensions,
        memberType: 'dimension',
        tableName: tableSchema.name,
        knownTableNames,
        referencedMembers,
        descriptors,
      });

      await ensureDescriptorBatchAlias({
        descriptors,
        ensureExpressionAlias,
        tableName: tableSchema.name,
      });

      return aliasedTableSchema;
    })
  );
};
