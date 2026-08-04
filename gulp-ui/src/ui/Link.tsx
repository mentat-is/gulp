import { Application } from '@/context/Application.context'
import { DisplayGroupDialog } from '@/dialogs/Group.dialog'
import { Point } from './Point'
import { Link } from '@/entities/Link'
import { DisplayEventDialog } from '@/dialogs/Event.dialog'
import { DataStore } from '@/store/DataStore'

export namespace LinkPoint {
  export interface Props extends Omit<Point.Props, 'icon' | 'name' | 'accent'> {
    link: Link.Type
  }
}

export function LinkPoint({ link, ...props }: LinkPoint.Props) {
  const { spawnDialog } = Application.use()

  /**
   * Opens the event or linked-events group represented by this link point.
   *
   * @returns Nothing when the link has no connected documents.
   */
  const openEvent = async (): Promise<void> => {
    if (link.doc_ids.length === 0) {
      return
    }

    const documentIds = link.doc_ids.length === 1
      ? [link.doc_id_from]
      : link.doc_ids
    const events = await DataStore.Events.getByIds(documentIds)
    if (events.length === 0) return

    const dialog = events.length === 1
      ? <DisplayEventDialog event={events[0]} />
      : (
        <DisplayGroupDialog
          events={events}
          groupKind="links"
        />
      )

    spawnDialog(dialog)
  }

  return (
    // @ts-ignore
    <Point
      onClick={openEvent}
      icon={Link.Entity.icon(link)}
      name={link.name}
      accent={link.color}
      {...props}
    />
  )
}
