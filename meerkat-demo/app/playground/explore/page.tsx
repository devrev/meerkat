'use client';

import { cubeQueryToSQL } from '@devrev/meerkat-browser';
import { JoinNode, TableSchema } from '@devrev/meerkat-core';
import { ColumnDef } from '@tanstack/react-table';
import { useState } from 'react';
import { DataTable } from '../../../components/data-table';
import { useDBM } from '../../../hooks/dbm-context';
import { useDefaultFileLoader } from '../files/hooks/use-default-file-loader';
import { WidgetBuilder } from './components/widget-builder';
import { useListDataSources } from './hooks/use-list-data-sources';

export function Explore() {
  const { dataSources } = useListDataSources();
  const { dbm, instanceManager } = useDBM();
  const isFileLoader = useDefaultFileLoader();
  const [tableData, setTableData] = useState<{
    rows: any[];
    columns: ColumnDef<any>[];
  }>({
    rows: [],
    columns: [],
  });

  return (
    <div className="grid h-full lg:grid-cols-12 m-4">
      <div className="col-span-3 border-r">
        <WidgetBuilder
          onChange={(options) => {
            (async () => {
              const db = await instanceManager.getDB();
              const connection = await db.connect();
              const measures = options.measures
                .filter((measure) => measure.checked)
                .map((measure) => {
                  return `${measure.dataSource}.${measure.name}`;
                });
              const dimensions = options.dimensions
                .filter((dimension) => dimension.checked)
                .map((dimension) => {
                  return `${dimension.dataSource}.${dimension.name}`;
                });
              // If no measures or dimensions are selected, return
              if (measures.length === 0 && dimensions.length === 0) {
                return;
              }

              const usedDataSourcesWithDuplicates: TableSchema[] = [];

              for (const measure of options.measures) {
                if (measure.checked) {
                  const dataSource = dataSources.find(
                    (dataSource) => dataSource.name === measure.dataSource
                  );
                  dataSource && usedDataSourcesWithDuplicates.push(dataSource);
                }
              }
              for (const dimension of options.dimensions) {
                if (dimension.checked) {
                  const dataSource = dataSources.find(
                    (dataSource) => dataSource.name === dimension.dataSource
                  );
                  dataSource && usedDataSourcesWithDuplicates.push(dataSource);
                }
              }

              // Remove duplicates via data source name
              const usedDataSources = usedDataSourcesWithDuplicates.filter(
                (dataSource, index, self) =>
                  index === self.findIndex((t) => t.name === dataSource.name)
              );

              const joinPaths: JoinNode[] = [];

              debugger;
              for (let i = 0; i < usedDataSources.length; i++) {
                const joins = usedDataSources[i].joins;
                if (!joins) continue;
                for (let j = 0; j < joins.length; j++) {
                  const join = joins[j].sql; // Example - orders.user_id = users.i
                  const joinSplit = join.split('=');
                  const left = joinSplit[0].trim();
                  const right = joinSplit[1].trim();
                  const leftSplit = left.split('.');
                  const rightSplit = right.split('.');
                  const leftDataSource = leftSplit[0];
                  const rightDataSource = rightSplit[0];
                  const leftColumn = leftSplit[1];
                  const rightColumn = rightSplit[1];
                  const leftDataSourceObj = usedDataSources.find(
                    (dataSource) => dataSource.name === leftDataSource
                  );
                  const rightDataSourceObj = usedDataSources.find(
                    (dataSource) => dataSource.name === rightDataSource
                  );
                  if (leftDataSourceObj && rightDataSourceObj) {
                    joinPaths.push({
                      left: leftDataSourceObj.name,
                      right: rightDataSourceObj.name,
                      on: leftColumn,
                    });
                  }
                }
              }

              console.info(usedDataSources);

              debugger;

              const sql = await cubeQueryToSQL(
                connection,
                {
                  measures: measures,
                  dimensions: dimensions,
                  joinPaths: [joinPaths],
                },
                usedDataSources
              );

              debugger;

              const result: any = await dbm.queryWithTables({
                query: sql,
                tables: usedDataSources,
                options: {
                  preQuery: async () => {
                    for (let i = 0; i < usedDataSources.length; i++) {
                      const dataSource = usedDataSources[i];
                      await dbm.query(
                        `CREATE OR REPLACE TABLE ${dataSource.name} AS SELECT * FROM read_parquet('${dataSource.name}.parquet')`
                      );
                    }
                  },
                },
              });
              const parsedOutputQuery = result
                .toArray()
                .map((row: any) => row.toJSON());

              //Convert all the BigInt to string
              for (let i = 0; i < parsedOutputQuery.length; i++) {
                for (const key in parsedOutputQuery[i]) {
                  if (typeof parsedOutputQuery[i][key] === 'bigint') {
                    parsedOutputQuery[i][key] =
                      parsedOutputQuery[i][key].toString();
                  }
                }
              }

              console.info(parsedOutputQuery);

              // Get keys from first row
              const firstRow = parsedOutputQuery[0];
              const columns: ColumnDef<any> = Object.keys(firstRow).map(
                (key) => ({
                  id: key,
                  header: key,
                  accessorKey: key,
                  cell: (prop) => {
                    console.log('prop', prop, key, prop.getValue());
                    return (
                      <div className="capitalize">
                        {parsedOutputQuery[prop.row.index][key]}
                      </div>
                    );
                  },
                })
              );
              setTableData({
                rows: parsedOutputQuery,
                columns,
              });
            })();
          }}
          dataSource={dataSources}
        />
      </div>

      <div className="col-span-9">
        {tableData.rows.length > 0 && (
          <DataTable
            columns={tableData.columns}
            data={tableData.rows}
          ></DataTable>
        )}
      </div>
    </div>
  );
}

export default Explore;
