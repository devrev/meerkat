'use client';

import { ColumnDef } from '@tanstack/react-table';
import { useState } from 'react';
import { DataTable } from '../../../../components/data-table';
import { useDBM } from '../../../../hooks/dbm-context';
import { useClassicEffect } from '../../../../hooks/use-classic-effect';
import { parseDuckdbArrowOutput } from '../../../../utils/utils';
export interface FilesProps {
  params: {
    file: string;
  };
}

export function File({ params }: FilesProps) {
  const { dbm } = useDBM();
  const isFileLoader = true;

  const [tableData, setTableData] = useState<{
    rows: any[];
    columns: ColumnDef<any>[];
  }>({
    rows: [],
    columns: [],
  });

  useClassicEffect(() => {
    (async () => {
      const tableName = params.file.split('.')[0];

      const query = `SELECT * FROM ${tableName} LIMIT 100`;
      const result: any = await dbm.queryWithTables({
        query,
        tables: [
          {
            name: tableName,
          },
        ],
        options: {
          preQuery: async () => {
            await dbm.query(
              `CREATE or REPLACE VIEW ${tableName} AS SELECT * FROM read_parquet('${params.file}')`
            );
          },
        },
      });
      const parsedOutputQuery = parseDuckdbArrowOutput(result);
      // Get keys from first row
      const firstRow = parsedOutputQuery[0];
      const columns: ColumnDef<any> = Object.keys(firstRow).map((key) => ({
        id: key,
        accessorKey: key,
        cell: (prop) => {
          return (
            <div className="capitalize">
              {parsedOutputQuery[prop.row.index][key]}
            </div>
          );
        },
      }));
      setTableData({
        rows: parsedOutputQuery,
        columns,
      });
    })();
  }, [isFileLoader]);

  if (!isFileLoader) {
    return <div>Loading file</div>;
  }

  console.info('tableData', tableData);

  return (
    <div className="grid h-full lg:grid-cols-12">
      <div className="col-span-10">
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

export default File;
