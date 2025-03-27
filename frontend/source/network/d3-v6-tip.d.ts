import type { Selection, BaseType } from "d3";

declare module "d3-v6-tip" {
  export interface D3Tip<GElement extends BaseType, Datum> {
    (selection: Selection<GElement, Datum, SVGElement, unknown>): void;
    attr(name: string, value: string): this;
    direction(dir: string): this;
    offset(arr: [number, number]): this;
    html(fn: (d: Datum) => string): this;
    show(d: Datum, node: GElement): void;
    hide(): void;
  }

  export function tip<GElement extends BaseType, Datum>(): D3Tip<
    GElement,
    Datum
  >;
}
