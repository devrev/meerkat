import { TableSchema } from '@devrev/meerkat-core';
import { useEffect, useMemo, useState } from 'react';
import { Badge } from '../../../../ui/badge';
import { Separator } from '../../../../ui/separator';
import {
  DropdownMenuCheckboxes,
  DropdownMenuCheckboxesProps,
} from './dropdown-picker';

interface Option {
  name: string;
  dataSource: string;
  checked?: boolean;
}

interface WidgetBuilderProps {
  dataSource: TableSchema[];
  onChange: (options: { measures: Option[]; dimensions: Option[] }) => void;
}

export const WidgetBuilder = ({ dataSource, onChange }: WidgetBuilderProps) => {
  const { measures, dimensions } = useMemo(() => {
    const measures = dataSource.flatMap((dataSource) => {
      return dataSource.measures.map((measure) => {
        return {
          ...measure,
          dataSource: dataSource.name,
        };
      });
    });
    const dimensions = dataSource.flatMap((dataSource) => {
      return dataSource.dimensions.map((dimension) => {
        return {
          ...dimension,
          dataSource: dataSource.name,
        };
      });
    });
    return { measures, dimensions };
  }, [dataSource]);

  const [measureOptions, setMeasureOptions] = useState<
    DropdownMenuCheckboxesProps['options']
  >([]);
  const [dimensionOptions, setDimensionOptions] = useState<
    DropdownMenuCheckboxesProps['options']
  >([]);
  useEffect(() => {
    setMeasureOptions(
      measures.map((item) => {
        return {
          name: item.name,
          dataSource: item.dataSource,
          checked: false,
        };
      })
    );
  }, [measures]);

  useEffect(() => {
    setDimensionOptions(
      dimensions.map((item) => {
        return {
          name: item.name,
          dataSource: item.dataSource,
          checked: false,
        };
      })
    );
  }, [dimensions]);

  useEffect(() => {
    onChange({
      measures: measureOptions,
      dimensions: dimensionOptions,
    });
  }, [measureOptions, dimensionOptions]);

  const selectedMeasures = measureOptions.filter((item) => item.checked);
  const selectedDimensions = dimensionOptions.filter((item) => item.checked);

  return (
    <div>
      <h1>Widget Builder</h1>

      <div className="py-2">
        <div className="pb-4 space-x-1 space-y-1">
          {selectedMeasures.map((item) => (
            <Badge variant="secondary" key={item.name + item.dataSource}>
              {item.dataSource}.{item.name}
            </Badge>
          ))}
        </div>

        <DropdownMenuCheckboxes
          dropdownName="Measures"
          onChange={(option) => {
            setMeasureOptions((prev) => {
              return prev.map((item) => {
                if (
                  item.name === option.name &&
                  item.dataSource === option.dataSource
                ) {
                  return {
                    ...item,
                    checked: option.checked,
                  };
                }
                return item;
              });
            });
          }}
          options={measureOptions}
        />
      </div>

      <Separator />

      <div className="py-2">
        <div className="pb-4 space-x-1 space-y-1">
          {selectedDimensions.map((item) => (
            <Badge variant="secondary" key={item.name + item.dataSource}>
              {item.dataSource}.{item.name}
            </Badge>
          ))}
        </div>

        <DropdownMenuCheckboxes
          dropdownName="Dimensions"
          onChange={(option) => {
            setDimensionOptions((prev) => {
              return prev.map((item) => {
                if (
                  item.name === option.name &&
                  item.dataSource === option.dataSource
                ) {
                  return {
                    ...item,
                    checked: option.checked,
                  };
                }
                return item;
              });
            });
          }}
          options={dimensionOptions}
        />
      </div>
    </div>
  );
};
