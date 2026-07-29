import { useCallback, useEffect } from 'react'
import s from './styles/Dialog.module.css'
import { cn } from '@/ui/utils'
import { Application } from '@/context/Application.context'
import { Stack } from './Stack'
import { Spinner } from './Spinner'

export namespace Dialog {
  export interface Props extends Stack.Props {
    loading?: boolean
    callback?: () => void
  }
}

export function Dialog({
  className,
  callback,
  loading,
  children,
  ...props
}: Dialog.Props) {
  const { Info, banner, spawnDialog, currentDocument } = Application.use()

  const close = useCallback(() => {
    if (callback) {
      callback()
    }

    spawnDialog(null)
    Info.setTimelineTarget(null)
  }, [Info, callback, spawnDialog])

  const handleDialogClose = useCallback((event: KeyboardEvent) => {
    if (event.key === 'Escape') {
      if (banner) {
        return;
      }
      close()
    }
  }, [banner, close])

  useEffect(() => {
    currentDocument.addEventListener('keydown', handleDialogClose)

    return () => {
      currentDocument.removeEventListener('keydown', handleDialogClose)
    }
  }, [currentDocument, handleDialogClose])

  return (
    <Stack className={cn(s.dialog, className)} dir="column" ai='stretch' pos='relative' {...props}>
      <Stack dir='column' gap={12} ai='stretch' className={cn(s.content, loading && s.loading)} data-content>
        {loading ? <Spinner size={24} /> : children}
      </Stack>
    </Stack>
  )
}
