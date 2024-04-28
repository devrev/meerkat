import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatDate(input: string | number): string {
  const date = new Date(input);
  return date.toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });
}

export function absoluteUrl(path: string) {
  return `${process.env.NEXT_PUBLIC_APP_URL}${path}`;
}

export const parseDuckdbArrowOutput = (arrowRows: any) => {
  const parsedOutputQuery = arrowRows.toArray().map((row: any) => row.toJSON());

  //Convert all the BigInt to float
  for (let i = 0; i < parsedOutputQuery.length; i++) {
    for (const key in parsedOutputQuery[i]) {
      if (typeof parsedOutputQuery[i][key] === 'bigint') {
        parsedOutputQuery[i][key] = parseFloat(parsedOutputQuery[i][key]);
      }
    }
  }

  return parsedOutputQuery as Record<string, any>[];
};
