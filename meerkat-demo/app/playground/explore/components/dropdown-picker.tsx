'use client';

import { DropdownMenuCheckboxItemProps } from '@radix-ui/react-dropdown-menu';

import { Button } from '../../../../ui/button';
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '../../../../ui/dropdown-menu';

type Checked = DropdownMenuCheckboxItemProps['checked'];

export interface DropdownMenuCheckboxesProps {
  options: {
    name: string;
    dataSource: string;
    checked?: boolean;
  }[];
  dropdownName: string;
  onChange: ({
    name,
    checked,
    dataSource,
  }: {
    name: string;
    checked: boolean;
    dataSource: string;
  }) => void;
}

export function DropdownMenuCheckboxes({
  options,
  onChange,
  dropdownName,
}: DropdownMenuCheckboxesProps) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline">Select {dropdownName}</Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent className="w-56">
        <DropdownMenuLabel>{dropdownName}</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {options.map((option) => (
          <DropdownMenuCheckboxItem
            key={option.name + option.dataSource}
            checked={option.checked}
            onCheckedChange={(checked) => {
              onChange({
                name: option.name,
                checked,
                dataSource: option.dataSource,
              });
            }}
          >
            <span>
              {option.dataSource}.{option.name}
            </span>
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
