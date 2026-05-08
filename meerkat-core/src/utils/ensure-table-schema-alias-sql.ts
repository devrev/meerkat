import { Dimension, Measure, TableSchema } from '../types/cube-types';

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

const collectAliasableDescriptors = ({
  members,
  memberType,
  tableName,
  knownTableNames,
  descriptors,
}: {
  members: AliasableMember[];
  memberType: MemberType;
  tableName: string;
  knownTableNames?: Set<string>;
  descriptors: AliasableMemberDescriptor[];
}): void => {
  members.forEach((member) => {
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
  ensureExpressionAlias,
}: EnsureTableSchemaAliasSqlParams): Promise<TableSchema[]> => {
  const knownTableNames = new Set(tableSchemas.map((s) => s.name));

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
        descriptors,
      });
      collectAliasableDescriptors({
        members: aliasedTableSchema.dimensions,
        memberType: 'dimension',
        tableName: tableSchema.name,
        knownTableNames,
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
