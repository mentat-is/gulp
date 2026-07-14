import { cn } from "@/ui/utils";
import { HTMLAttributes } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypePrismPlus from "rehype-prism-plus";
import s from "./styles/Markdown.module.css";
import "./styles/prism-tomorrow.css";

export namespace Markdown {
  export interface Props extends Omit<HTMLAttributes<HTMLDivElement>, "children"> {
    value: string;
    scrollable?: boolean;
  }
}

export function Markdown({ className, value, scrollable = true, ...props }: Markdown.Props) {
  return (
    <div className={cn(s.markdown, !scrollable && s.noScroll, className)} {...props}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypePrismPlus]}
      >
        {value}
      </ReactMarkdown>
    </div>
  );
}
