import React, { ReactNode } from 'react'
import s from './styles/Card.module.css'
import { cn } from '@/ui/utils'

export namespace Card {
  export interface Props {
    description?:
    | string
    | {
      text: string
      button: React.ReactNode
    }
    children: ReactNode
    className?: string | string[]
  }
}

export function Card({ description, children, className }: Card.Props) {
  return (
    <div className={cn(className, s._)}>
      <div className={s.content}>{children}</div>
      {description && (
        <div className={s.description}>
          {typeof description === 'string' ? (
            <p>{description}</p>
          ) : (
            <React.Fragment>
              <p>{description.text}</p>
              {description.button}
            </React.Fragment>
          )}
        </div>
      )}
    </div>
  )
}
