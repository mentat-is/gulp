import { ElementType, HTMLAttributes, ReactNode } from "react";
import s from './styles/Shimmer.module.css';
import { cn } from "@/ui/utils";
import { Refractor } from "./utils";

export namespace Shimmer {
  export interface Props<T extends ElementType = "p"> extends HTMLAttributes<any> {
    as?: T
    value?: ReactNode
    children?: ReactNode
    duration?: number
  }
}

export function Shimmer<T extends ElementType = "p">({
  as,
  value,
  children,
  className,
  duration = 4,
  color = 'var(--second)',
  ...props
}: Shimmer.Props<T>) {
  const Component = as || 'p';
  return (
    <Component className={cn(className, s.shimmer)} style={Refractor.reflect.toVar({ duration, color })} {...props}>
      {value ?? children}
    </Component>
  );
}
