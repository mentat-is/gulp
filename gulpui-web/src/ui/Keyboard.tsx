import React,{ HTMLAttributes } from 'react';
import s from './styles/Keyboard.module.css';
import { cn } from '@/ui/utils';

export function Keyboard({ meta, shift, alt, ctrl, plus, command, className, children, ...props }: Keyboard.Props) {
  const keys = Keyboard.get({ meta, shift, alt, ctrl, command });

  if (children) {
    keys.push(children.toString() as typeof Keyboard.Keys[Keyboard.Key]);
  }

  return (
    <kbd className={cn(className, s.kbd)} {...props}>
      {keys.map((key, i) => (
        <React.Fragment key={i}>
         {key} {plus && i !== keys.length - 1 && '+'}
        </React.Fragment>
      ))}
    </kbd>
  )
}

export namespace Keyboard {
  export interface Props extends HTMLAttributes<HTMLUnknownElement> {
    meta?: true;
    shift?: true;
    alt?: true;
    ctrl?: true;
    plus?: true;
    command?: true;
  }

  export const Keys = {
    meta: 'Ctrl',
    shift: '⇧',
    alt: '⌥',
    ctrl: '⌃',
    command: '⌘'
  } as const;

  export type Key = keyof typeof Keyboard.Keys;

  export function get(keys: Record<Keyboard.Key, boolean | undefined>) {
    const arr = Object.keys(keys) as Keyboard.Key[];
    return arr.filter(key => keys[key] === true).map(key => Keyboard.Keys[key]).filter(exist => exist);
  }
}
