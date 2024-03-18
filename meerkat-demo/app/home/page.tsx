import Image from 'next/image';
import Link from 'next/link';
import { BackgroundBeams } from '../../ui/background-beams';
import { Button } from '../../ui/button';

export default function HomePage() {
  return (
    <div className="relative isolate pt-4">
      <div className="py-24 sm:py-32 lg:pb-40 bg-neutral-950">
        <div className="w-full rounded-md relative flex flex-col flex-grow items-center justify-center antialiased">
          <div className="mx-auto max-w-4xl px-2 md:px-6 lg:px-8">
            <div className="mx-auto max-w-2xl text-center">
              <h1 className="relative z-10 text-4xl md:text-7xl bg-clip-text text-transparent bg-gradient-to-b from-neutral-200 to-neutral-600 text-center font-sans font-bold">
                Meerkat
              </h1>
              <p className="text-neutral-500 max-w-lg mx-auto my-4 text-lg text-center relative z-10">
                Meerkat is TypeScript SDK solution that seamlessly converts
                Cube-like queries to DuckDB queries. Whether you're using a
                browser or NodeJS, Meerkat delivers a powerful layer of
                abstraction for executing advanced queries.
              </p>
              <Link href="/playground/explore" passHref>
                <Button className="z-10  relative inline-flex h-12 animate-shimmer items-center justify-center border border-slate-800 bg-[linear-gradient(110deg,#000103,45%,#1e2631,55%,#000103)] bg-[length:200%_100%] px-6 font-medium text-slate-400 transition-colors focus:outline-none focus:ring-2 focus:ring-slate-400 focus:ring-offset-2 focus:ring-offset-slate-50 rounded-lg mt-12 hover:cursor-pointer">
                  Open Playground
                </Button>
              </Link>
              <Image
                className="mx-auto mt-12 rounded-md"
                src="/meerkat.png"
                alt="meerkat"
                width={672}
                height={372}
              />
            </div>
          </div>
          <BackgroundBeams />
        </div>
      </div>
    </div>
  );
}
