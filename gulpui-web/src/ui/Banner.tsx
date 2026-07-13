import { ReactNode, useContext, useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { Application } from '../context/Application.context'
import s from './styles/Banner.module.css'
import { cn } from '@/ui/utils'
import { Stack } from './Stack'
import { Skeleton } from './Skeleton'
import { Button } from './Button'

export namespace Banner {
  export interface Props extends Stack.Props {
    title?: string
    subtitle?: ReactNode
    done?: ReactNode
    side?: ReactNode
    fixed?: boolean
    loading?: boolean
    onClose?: () => void
    option?: ReactNode | null
    back?: () => void | null
    container?: HTMLElement | null
  }
}

export function Banner({
  children,
  back,
  className,
  title,
  fixed,
  option,
  loading,
  done,
  subtitle = null,
  side = null,
  onClose,
  container,
}: Banner.Props) {
  const ctx = useContext(Application.Context)
  const { destroyBanner, currentDocument } = ctx
  const [isExpanded, setIsExpanded] = useState<boolean>(false)

  const close = () => {
    if (onClose) onClose()

    destroyBanner()
  }

  useEffect(() => {
    if (fixed) return

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        close()
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    window.addEventListener('popstate', close)

    return () => {
      window.removeEventListener('keydown', handleKeyDown)
      window.removeEventListener('popstate', close)
    }
  }, [])

  const Side = useMemo(() => {
    if (!side) {
      return null
    }

    return (
      <Stack className={cn(s.side, isExpanded && s.open)} pos="absolute">
        <Stack className={s.side_content} ai='flex-start' pos="relative">
          {side}
        </Stack>
      </Stack>
    )
  }, [side, isExpanded, setIsExpanded])

  const resolvedContainer = container || currentDocument?.body || globalThis.document?.body;

  if (!resolvedContainer) return null;

  return createPortal(
    <div className={s.wrapper}>
      <div
        data-expanded={isExpanded}
        className={cn(s.banner, s.loading, className)}>
        <Stack dir="row" ai="center" jc="space-between" className={s.header}>
          {title && <h6>
            {loading ? <Skeleton width="long" height={24} /> : title}

          </h6>}
          <Stack dir="row" ai="center" gap={8}>
            {subtitle ? loading ? <Skeleton height={24} /> : subtitle : null}
            {back && <Button variant='tertiary' icon="CornerUpLeft" onClick={back} />}
            {fixed ? null : (
              <Button
                variant='glass'
                onClick={close}
                icon="X"
                loading={loading}
                shape='icon'
              />
            )}
          </Stack>
        </Stack>
        <Stack dir="column" ai="unset" gap={12} className={s.content} data-banner-content>
          {Side}
          {children}
        </Stack>
        {(side || option || done) && (
          <Stack dir="row" ai="center" jc="flex-end" gap={12} className={s.footer}>
            {side ? (
              <Button
                icon={isExpanded ? 'Eye' : 'EyeOff'}
                onClick={() => setIsExpanded((isExpanded) => !isExpanded)}
                variant={isExpanded ? 'secondary' : 'tertiary'}
              />
            ) : (
              option
            )}
            {done}
          </Stack>
        )}
      </div>
    </div>,
    resolvedContainer
  )
}
