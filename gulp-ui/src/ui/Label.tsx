import { cn } from "@/ui/utils";
import { CSSProperties, LabelHTMLAttributes } from "react";
import s from './styles/Label.module.css'

export namespace Label {
  export interface Props extends LabelHTMLAttributes<HTMLLabelElement> {
    value?: string;
    cursor?: CSSProperties['cursor'];
  }
}

export function Label({ value, className, style, cursor = 'text', ...props }: Label.Props) {
  if (!value) {
    return null;
  }

  return (
    <label className={cn(s.label, className)} style={{ cursor, ...style }} {...props}>{value}</label>
  )
}